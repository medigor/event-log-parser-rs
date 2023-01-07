use event_log_parser::{events, references::References};

#[test]
fn test_files() {
    let mut refs = References::new();
    refs.parse("../test-log/1Cv8.lgf").unwrap();

    assert_eq!(refs.users()[2].name(), "Андрей Кудрявцев");
    assert_eq!(refs.computers()[1], "computer1");

    let mut total_events = 0;
    events::parse("../test-log/20221212000000.lgp", &mut |event| {
        let _date = event.date();

        if total_events == 11 {
            assert_eq!(event.user(&refs).name(), "Андрей Кудрявцев");
            assert_eq!(event.computer(&refs), "computer1");
            assert_eq!(event.event(&refs), "_$Data$_.Update");
            assert_eq!(
                event.metadata(&refs).name(),
                "Константа.ИдентификаторИнформационнойБазы"
            );
        } else if total_events == 24 {
            assert_eq!(event.event(&refs), "Полнотекстовое индексирование");
            assert!(event
                .comment()
                .starts_with("Не удалось проверить состояние индекса полнотекстового поиска"));
            assert!(event
                .comment()
                .contains(r#"Выполнить ИмяМетода + "(" + ПараметрыСтрока + ")";"#));
        }

        total_events += 1;
    })
    .unwrap();

    assert_eq!(total_events, 1274);
}
