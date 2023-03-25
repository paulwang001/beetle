FROM rust:latest AS builder
WORKDIR /app
COPY ./luffa-relay .
#RUN apt-get update
#RUN apt-get install curl git gcc g++ libssl-dev pkg-config -y
#RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
#COPY config $HOME/.cargo/config
RUN cargo build --bin main --release

FROM debian
ENV PARAMS=dev
WORKDIR /app
COPY --from=builder /app/target/release/main .
COPY --from=builder /app/cfg cfg
CMD ["sh", "-c", "./main", "-e", "$PARAMS"]