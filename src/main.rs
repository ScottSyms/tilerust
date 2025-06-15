use std::fs::File;
use std::path::Path;
use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, TimeZone, Utc};
use actix_files as fs;
use actix_web::{get, web, App, HttpResponse, HttpServer, Responder};
use std::sync::{Arc, Mutex};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::Field;
use rstar::{RTree, RTreeObject, AABB};
use image::{ImageBuffer, Rgba};
use serde::Deserialize;
use walkdir::WalkDir;

fn debug_enabled() -> bool {
    std::env::var("DEBUG").map(|v| v == "1").unwrap_or(false)
}

macro_rules! debug_log {
    ($($arg:tt)*) => {
        if debug_enabled() {
            eprintln!($($arg)*);
        }
    };
}

#[derive(Clone, Copy, Debug, Deserialize)]
struct DataPoint {
    x: f64,
    y: f64,
}

impl RTreeObject for DataPoint {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        AABB::from_point([self.x, self.y])
    }
}

struct AppState {
    tree: Arc<Mutex<RTree<DataPoint>>>,
}

fn tile2mercator(xtile: u32, ytile: u32, zoom: u32) -> (f64, f64) {
    debug_log!("tile2mercator xtile={} ytile={} zoom={}", xtile, ytile, zoom);
    let n = 2f64.powi(zoom as i32);
    let lon_deg = xtile as f64 / n * 360.0 - 180.0;
    let lat_rad = ((std::f64::consts::PI * (1.0 - 2.0 * ytile as f64 / n)).sinh()).atan();
    let lat_deg = lat_rad.to_degrees();
    let res = lnglat_to_meters(lon_deg, lat_deg);
    debug_log!("tile2mercator result lon={} lat={} -> ({}, {})", lon_deg, lat_deg, res.0, res.1);
    res
}

fn lnglat_to_meters(lon: f64, lat: f64) -> (f64, f64) {
    debug_log!("lnglat_to_meters lon={} lat={}", lon, lat);
    // Web Mercator projection (EPSG:3857) conversion
    let origin_shift = std::f64::consts::PI * 6378137.0;

    // Easting calculation
    let x = lon * origin_shift / 180.0;

    // Northing calculation - note the 90+lat adjustment
    let y = ((90.0 + lat) * std::f64::consts::PI / 360.0)
        .tan()
        .ln()
        * origin_shift
        / std::f64::consts::PI;

    debug_log!("lnglat_to_meters result -> ({}, {})", x, y);
    (x, y)
}

fn generate_tile(zoom: u32, x: u32, y: u32, tree: &RTree<DataPoint>) -> Vec<u8> {
    debug_log!("generate_tile z={} x={} y={}", zoom, x, y);
    let (xleft, ytop) = tile2mercator(x, y, zoom);
    let (xright, ybottom) = tile2mercator(x + 1, y + 1, zoom);

    let bbox = AABB::from_corners([xleft, ybottom], [xright, ytop]);
    debug_log!("bbox: [{}, {}]-[{}, {}]", xleft, ybottom, xright, ytop);
    let points = tree.locate_in_envelope(&bbox);

    let width = 256u32;
    let height = 256u32;
    let mut counts = vec![0u32; (width * height) as usize];
    let mut point_count = 0u32;

    for p in points {
        let px = ((p.x - xleft) / (xright - xleft) * width as f64) as i32;
        let py = ((ytop - p.y) / (ytop - ybottom) * height as f64) as i32;
        if px >= 0 && px < width as i32 && py >= 0 && py < height as i32 {
            let idx = (py as u32 * width + px as u32) as usize;
            counts[idx] += 1;
        }
        point_count += 1;
    }

    debug_log!("points processed: {}", point_count);

    let max_count = counts.iter().copied().max().unwrap_or(0);
    debug_log!("max_count={}", max_count);

    let mut img = ImageBuffer::<Rgba<u8>, Vec<u8>>::new(width, height);
    if max_count > 0 {
        for (i, cnt) in counts.into_iter().enumerate() {
            // Use a logarithmic scale so areas of high density ramp up toward red
            let val = (cnt as f32).ln_1p() / (max_count as f32).ln_1p();
            let color = color_map(val);
            let x = (i as u32) % width;
            let y = (i as u32) / width;
            img.put_pixel(x, y, color);
        }
    }

    use std::io::Cursor;
    let mut bytes: Vec<u8> = Vec::new();
    {
        let mut cursor = Cursor::new(&mut bytes);
        image::DynamicImage::ImageRgba8(img)
            .write_to(&mut cursor, image::ImageFormat::Png)
            .unwrap();
    }
    bytes
}

fn color_map(v: f32) -> Rgba<u8> {
    if !v.is_finite() || v <= 0.0 {
        return Rgba([0, 0, 0, 0]);
    }
    // Intensify red as density increases
    let intensity = v.powf(0.5).clamp(0.0, 1.0);
    let r = (255.0 * intensity) as u8;
    let b = 255 - r;
    debug_log!("color_map v={} -> r={} b={}", v, r, b);
    Rgba([r, 0, b, 255])
}

fn load_points_from_dir<P: AsRef<Path>>(
    dir: P,
    start: Option<DateTime<Utc>>,
    end: Option<DateTime<Utc>>,
) -> Vec<DataPoint> {
    debug_log!("loading points from {:?} start={:?} end={:?}", dir.as_ref(), start, end);
    let mut all_points: Vec<(DataPoint, DateTime<Utc>)> = Vec::new();
    let mut max_time: Option<DateTime<Utc>> = None;

    for entry in WalkDir::new(dir).into_iter().filter_map(|e| e.ok()) {
        if entry
            .path()
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| e.eq_ignore_ascii_case("parquet"))
            .unwrap_or(false)
        {
            debug_log!("processing file {:?}", entry.path());
            if let Ok(file) = File::open(entry.path()) {
                if let Ok(reader) = SerializedFileReader::new(file) {
                    if let Ok(iter) = reader.get_row_iter(None) {
                        for record in iter {
                            if let Ok(row) = record {
                                let x = get_f64_by_name(&row, "longitude").unwrap_or(0.0);
                                let y = get_f64_by_name(&row, "latitude").unwrap_or(0.0);
                                if let Some(ts) = get_datetime_by_name(&row, "BaseDateTime") {
                                    if max_time.map(|m| ts > m).unwrap_or(true) {
                                        max_time = Some(ts);
                                    }
                                    debug_log!("row x={} y={} ts={:?}", x, y, ts);
                                    all_points.push((DataPoint { x, y }, ts));
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    if let Some(max) = max_time {
        let end_time = end.unwrap_or(max);
        let start_time = start.unwrap_or(end_time - Duration::hours(24));
        let result: Vec<DataPoint> = all_points
            .into_iter()
            .filter(|(_, ts)| *ts >= start_time && *ts <= end_time)
            .map(|(pt, _)| pt)
            .collect();
        debug_log!("points in range: {}", result.len());
        result
    } else {
        debug_log!("no points found");
        Vec::new()
    }
}

fn get_f64_by_name(row: &parquet::record::Row, name: &str) -> Option<f64> {
    for (n, field) in row.get_column_iter() {
        if n == name {
            let res = match field {
                Field::Double(v) => Some(*v),
                Field::Float(v) => Some(*v as f64),
                Field::Int(v) => Some(*v as f64),
                Field::Long(v) => Some(*v as f64),
                Field::UInt(v) => Some(*v as f64),
                Field::ULong(v) => Some(*v as f64),
                _ => None,
            };
            debug_log!("get_f64_by_name {} -> {:?}", name, res);
            return res;
        }
    }
    None
}

fn get_datetime_by_name(row: &parquet::record::Row, name: &str) -> Option<DateTime<Utc>> {
    for (n, field) in row.get_column_iter() {
        if n == name {
            let res = match field {
                Field::TimestampMillis(v) => DateTime::from_timestamp_millis(*v).map(|dt| dt.with_timezone(&Utc)),
                Field::TimestampMicros(v) => DateTime::from_timestamp_micros(*v).map(|dt| dt.with_timezone(&Utc)),
                Field::Int(v) => DateTime::from_timestamp(*v as i64, 0).map(|dt| dt.with_timezone(&Utc)),
                Field::Long(v) => DateTime::from_timestamp(*v, 0).map(|dt| dt.with_timezone(&Utc)),
                Field::Date(v) => {
                    NaiveDateTime::from_timestamp_opt((*v as i64) * 86_400, 0)
                        .map(|nd| Utc.from_utc_datetime(&nd))
                }
                Field::Str(s) => {
                    DateTime::parse_from_rfc3339(s)
                        .map(|dt| dt.with_timezone(&Utc))
                        .or_else(|_| {
                            NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                                .map(|nd| Utc.from_utc_datetime(&nd))
                        })
                        .ok()
                }
                _ => None,
            };
            debug_log!("get_datetime_by_name {} -> {:?}", name, res);
            return res;
        }
    }
    None
}

fn parse_input_datetime(s: &str, end_of_day: bool) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .or_else(|_| {
            NaiveDate::parse_from_str(s, "%Y-%m-%d").map(|d| {
                let dt = if end_of_day {
                    d.and_hms_opt(23, 59, 59).unwrap()
                } else {
                    d.and_hms_opt(0, 0, 0).unwrap()
                };
                DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc)
            })
        })
        .ok()
}

#[get("/")]
async fn index() -> impl Responder {
    debug_log!("serving index.html");
    fs::NamedFile::open("./www/index.html")
}

#[get("/tiles/{zoom}/{x}/{y}.png")]
async fn tile(path: web::Path<(u32, u32, u32)>, data: web::Data<AppState>) -> HttpResponse {
    let (z, x, y) = path.into_inner();
    debug_log!("tile request z={} x={} y={}", z, x, y);
    let tree = data.tree.lock().unwrap();
    let img = generate_tile(z, x, y, &tree);
    HttpResponse::Ok().content_type("image/png").body(img)
}

#[derive(Deserialize)]
struct RangeParams {
    start: Option<String>,
    end: Option<String>,
}

#[get("/range")]
async fn range(query: web::Query<RangeParams>, data: web::Data<AppState>) -> HttpResponse {
    let start = query
        .start
        .as_deref()
        .and_then(|s| parse_input_datetime(s, false));
    let end = query
        .end
        .as_deref()
        .and_then(|s| parse_input_datetime(s, true));
    let points = load_points_from_dir("partition", start, end);
    let tree = RTree::bulk_load(points);
    {
        let mut t = data.tree.lock().unwrap();
        *t = tree;
    }
    HttpResponse::Ok().body("ok")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    debug_log!("starting server");
    let base_path = "partition";
    let points = load_points_from_dir(base_path, None, None);
    debug_log!("loaded {} points", points.len());
    let tree = Arc::new(Mutex::new(RTree::bulk_load(points)));
    let data = web::Data::new(AppState { tree });

    HttpServer::new(move || {
        App::new()
            .app_data(data.clone())
            .service(index)
            .service(tile)
            .service(range)
            .service(fs::Files::new("/lib", "./www/lib"))
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}
