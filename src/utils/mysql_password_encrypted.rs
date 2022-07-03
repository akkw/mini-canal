pub fn scramble323(password: Option<&str>, seed: Option<&str>) -> String {
    if password == Option::None || password.unwrap().len() == 0 {
        return String::from(password.unwrap());
    }
    let mut b = 0;
    let mut d = 0f64;
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
        *c = (*c) as u8 ^ b as u8;
        i += 1;
    }
    String::from(std::str::from_utf8(chars.as_slice()).unwrap())
}

// byte[] pass1 = md.digest(pass);
// md.reset();
// byte[] pass2 = md.digest(pass1);
// md.reset();
// md.update(seed);
// byte[] pass3 = md.digest(pass2);

pub fn scramble411 (password: &[u8], seed: &[u8]) -> Box<[u8]>{
    let mut sha1 = sha1_smol::Sha1::new();
    sha1.update(password);
    let pass1 = sha1.digest().bytes();
    sha1.reset();

    sha1.update(pass1.as_ref());
    let pass2 = sha1.digest().bytes();
    sha1.reset();

    sha1.update(seed);
    sha1.update(pass2.as_ref());

    let mut pass3 = sha1.digest().bytes();

    for i in 0..pass3.len() {
        pass3[i] =  pass3[i] ^ pass1[i];
    }
    Box::from(pass3)

}


pub fn scrambleCachingSha2 (password: &[u8], seed: &[u8]) -> Box<[u8]>{
    let mut sha1 = sha1_smol::Sha1::new();
    sha1.update(password);
    let pass1 = sha1.digest().bytes();
    sha1.reset();

    sha1.update(pass1.as_ref());
    let pass2 = sha1.digest().bytes();
    sha1.reset();

    sha1.update(seed);
    sha1.update(pass2.as_ref());

    let mut pass3 = sha1.digest().bytes();

    for i in 0..pass3.len() {
        pass3[i] =  pass3[i] ^ pass1[i];
    }
    Box::from(pass3)

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
                tmp = 0xff & c as i64;
                nr ^= (((nr & 63) + add) * tmp) + (nr << 8);
                nr2 += (nr2 << 8) ^ nr;
                add += tmp;
            }
        }
    }

    let mut result = [0 as i64, 2];
    result[0] = nr & 0x7fffffff;
    result[1] = nr2 & 0x7fffffff;
    Box::new(result)
}