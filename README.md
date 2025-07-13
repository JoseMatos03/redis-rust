# Redis in Rust

This project is my personal implementation of a toy Redis clone, built entirely in **Rust**, as part of the [CodeCrafters "Build Your Own Redis" challenge](https://codecrafters.io/challenges/redis). The goal is to rebuild key features of Redis from scratch, with the main goal of developing my Rust programming skills.

---

## Getting Started

To run the Redis clone locally:

```sh
./your_program.sh
```

> This will build and run the server. The first build might take a while; Rust compiles fast after that.

Make sure you have `cargo` installed (tested with `cargo 1.88` or later).

---

## What's Implemented

- [x] RESP parsing
- [x] Basic command handling (`PING`, `ECHO`, `SET`, `GET`)
- [x] Expiry support
- [x] RDB configuration
- [ ] RDB persistence

More features will be added as I progress through the challenge.

---

© 2025 José Matos. All rights reserved.
