# event-log-parser-rs
Парсер **Журнала регистрации 1С:Предприятие 8** на языке **Rust**

Пример использования см. [parser/tests](parser/tests) и [examples](examples)

Проверить скорость:
```bash
cargo run --release --bin get-statistic /path/to/1Cv8Log
```