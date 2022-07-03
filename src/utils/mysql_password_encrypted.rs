pub fn scramble323(password: Option<&str>, seed: Option<&str>) -> String {
    if password == Option::None || password.unwrap().len() == 0 {
        return String::from(password.unwrap());
    }
    let mut b: u8 = 0;
    let mut d: f64 = 0f64;
    let pw = hash(seed.unwrap());
    let msg = hash(password.unwrap());
    let max: i64 = 0x3fffffff;
    let mut seed1: i64 = ((pw[0] ^ msg[0]) as i64 % max) as i64;
    let mut seed2: i64 = ((pw[1] ^ msg[1]) as i64 % max) as i64;
    let mut chars = vec![];
    for _char in seed.unwrap().chars() {
        seed1 = ((seed1 * 3) + seed2) % max;
        seed2 = (seed1 + seed2 + 33) % max;
        d = seed1 as f64 / max as f64;
        b = (((d * 31 as f64) + 64 as f64) as i64) as u8;
        chars.push(b);
    }

    seed1 = ((seed1 * 3) + seed2) % max;
    seed2 = (seed1 + seed2 + 33) % max;
    d = seed1 as f64 / max as f64;
    b = (d * 31 as f64) as u8;
    let mut i = 0;
    for c in &mut chars {
        *c = ((*c) as u8 ^ b as u8);
        i += 1;
    }
    String::from(std::str::from_utf8(chars.as_slice()).unwrap())
}


fn hash(str: &str) -> Box<[i64]> {
    let mut nr: i64 = 1345345333;
    let mut add: i64 = 7;
    let mut nr2: i64 = 0x12345671;
    let mut tmp: i64 = 0;

    for c in str.chars() {
        match c {
            ' ' => {}
            '\t' => { continue; }
            _ => {
                tmp = (0xff & c as i64);
                nr ^= ((((nr & 63) + add) * tmp) + (nr << 8));
                nr2 += ((nr2 << 8) ^ nr);
                add += tmp;
            }
        }
    }

    let mut result = [0 as i64, 2];
    result[0] = (nr & 0x7fffffff);
    result[1] = (nr2 & 0x7fffffff);
    Box::new(result)
}