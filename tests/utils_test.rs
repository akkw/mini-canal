extern crate core;

use mysql_binlog_parse::command::capability;
use mysql_binlog_parse::utils::mysql_password_encrypted;
use mysql_binlog_parse::utils::mysql_password_encrypted::{scramble323, scramble411};
#[test]
fn scramble323test() {
    assert_eq!(scramble323(Option::from("bar123\tbaz"), Option::from("a")), String::from("X"), "scramble323 eq pass");
    assert_ne!(scramble323(Option::from("bar123\tbaz"), Option::from("a")), String::from("U"), "scramble323 ne pass");
}

#[test]
fn scramble411test() {
    let bytes1: [u8; 20] = [90, 11, 237, 60, 27, 229, 22, 92, 218, 4, 40, 194, 156, 74, 17, 6, 115, 219, 137, 130];
    let bytes2: [u8; 20] = [144, 172, 198, 232, 168, 40, 205, 38, 38, 161, 110, 255, 41, 67, 51, 175, 76, 240, 184, 28];
    let mut scramble4111 = scramble411([].as_slice(), [].as_slice());
    assert_eq!(bytes1.as_slice(), scramble4111.as_mut());
    let mut scramble4111 = scramble411([114, 111, 111, 116].as_slice(), [37, 73, 41, 87, 22, 56, 51, 91, 105, 70, 125, 40, 21, 10, 18, 63, 1, 46, 29, 117].as_slice());
    assert_eq!(bytes2.as_slice(), scramble4111.as_mut());

}

