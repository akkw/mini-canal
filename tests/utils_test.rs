use mysql_binlog_parse::utils::mysql_password_encrypted;
use mysql_binlog_parse::utils::mysql_password_encrypted::scramble323;

#[test]
fn scramble323test() {
    assert_eq!(scramble323(Option::from("bar123\tbaz"), Option::from("a")), String::from("X"),"scramble323 eq pass");
    assert_ne!(scramble323(Option::from("bar123\tbaz"), Option::from("a")), String::from("U"), "scramble323 ne pass");
}

