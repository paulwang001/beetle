FROM rust:latest AS builder
WORKDIR /app
COPY . .
#RUN apt-get update
#RUN apt-get install curl git gcc g++ libssl-dev pkg-config -y
#RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
#COPY config $HOME/.cargo/config
RUN cargo build --bin luffa-relay --release

FROM debian
ENV PARAMS=luffa.boot.docker.toml
WORKDIR /app
COPY --from=builder /app/luffa-relay/luffa* .
COPY --from=builder /app/luffa-relay/cli* .
COPY --from=builder /app/target/release/luffa-relay .

CMD ["sh", "-c", "RUST_LOG=info ./luffa-relay", "--cfg", "$PARAMS"]