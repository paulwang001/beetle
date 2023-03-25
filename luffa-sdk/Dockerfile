FROM rust:latest AS builder
WORKDIR /app
COPY . .
#RUN apt-get update
#RUN apt-get install curl git gcc g++ libssl-dev pkg-config -y
#RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
COPY config $HOME/.cargo/config
RUN cargo build --release

FROM debian
ENV PARAMS=luffa.client.toml
WORKDIR /app
COPY --from=builder /app/luffa.client.toml .
COPY --from=builder /app/target/release/main .
COPY --from=builder /app/cfg cfg
CMD ["sh", "-c", "./main", "--cfg", "$PARAMS"]