FROM rust:1.58.0-buster as chef
RUN rustup component add rustfmt
RUN cargo install cargo-chef
WORKDIR app

FROM chef as planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef as builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release --bins

FROM debian:buster as runtime
RUN apt-get update && apt-get install sqlite3 -y
WORKDIR app
COPY --from=builder /app/target/release/ /usr/local/bin
ENTRYPOINT []