# tilerust

A simple tile server for visualizing large datasets of points, built with Rust.

This web service reads geospatial data from Parquet files, indexes it using an R-tree for efficient querying, and serves map tiles as PNG images. The color of the tiles represents the density of the data points.

## Features

*   **Fast Tile Generation:** Leverages Rust's performance and an R-tree for quick tile generation.
*   **Parquet File Support:** Reads data from Parquet files, a popular columnar storage format.
*   **Dynamic Filtering:** Filter data by a time range using the `/range` endpoint.
*   **Web Mercator Projection:** Converts longitude/latitude to Web Mercator coordinates for tile generation.
*   **Simple Frontend:** A basic frontend is provided in the `www` directory to visualize the tiles.

## Dependencies

*   `actix-web`: A powerful, pragmatic, and extremely fast web framework for Rust.
*   `actix-files`: Static file serving for Actix Web.
*   `image`: Image processing library for Rust.
*   `rstar`: An R*-tree library for Rust.
*   `parquet`: A Rust implementation of the Parquet file format.
*   `serde`: A framework for serializing and deserializing Rust data structures.
*   `walkdir`: A library for walking a directory tree.
*   `chrono`: A date and time library for Rust.

## Installation

1.  Clone the repository:
    ```bash
    git clone https://github.com/ScottSyms/tilerust.git
    cd tilerust
    ```

2.  Build the project:
    ```bash
    cargo build --release
    ```

## Usage

1.  Place your Parquet files in the `partition` directory. The service will recursively search for `.parquet` files in this directory. The Parquet files should contain `longitude`, `latitude`, and `BaseDateTime` columns.

2.  Run the server:
    ```bash
    cargo run --release
    ```

3.  Open your browser and navigate to `http://localhost:8080` to see the tiles.

### Endpoints

*   `GET /`: Serves the frontend.
*   `GET /tiles/{zoom}/{x}/{y}.png`: Generates and serves a map tile.
*   `GET /range?start=<start_date>&end=<end_date>`: Filters the data by a time range. The dates can be in `YYYY-MM-DD` or RFC3339 format.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## License

This project is licensed under the MIT License.