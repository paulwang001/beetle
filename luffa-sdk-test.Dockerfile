FROM rust:latest AS builder
WORKDIR /app
COPY . .
#RUN apt-get update
#RUN apt-get install curl git gcc g++ libssl-dev pkg-config -y
#RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
#COPY config $HOME/.cargo/config
RUN cargo build --bin luffa-client-test --release

#FROM debian
#ENV PARAMS=luffa.client.docker.toml
#WORKDIR /app
#COPY --from=builder /app/luffa-sdk/luffa* .
#COPY --from=builder /app/target/release/luffa-client .
#CMD ["sh", "-c", "RUST_LOG=info ./luffa-sdk-test", "--cfg", "$PARAMS"]