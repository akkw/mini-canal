pub struct AuthenticationInfo {
    address: String,
    port: u16,
    username: String,
    password: String,
    default_database_name: String,
    pwd_public_key: String,
    enable_druid: bool,
}

impl AuthenticationInfo {
    pub fn form(address: String, port: u16, username: String, password: String, default_database_name: String, pwd_public_key: String, enable_druid: bool) -> Self {
        Self { address, port, username, password, default_database_name, pwd_public_key, enable_druid }
    }


    pub fn address(&self) -> &str {
        &self.address
    }
    pub fn port(&self) -> u16 {
        self.port
    }
    pub fn username(&self) -> &str {
        &self.username
    }
    pub fn password(&self) -> &str {
        &self.password
    }
    pub fn default_database_name(&self) -> &str {
        &self.default_database_name
    }
    pub fn pwd_public_key(&self) -> &str {
        &self.pwd_public_key
    }
    pub fn enable_druid(&self) -> bool {
        self.enable_druid
    }
    pub fn set_address(&mut self, address: String) {
        self.address = address;
    }
    pub fn set_port(&mut self, port: u16) {
        self.port = port;
    }
    pub fn set_username(&mut self, username: String) {
        self.username = username;
    }
    pub fn set_password(&mut self, password: String) {
        self.password = password;
    }
    pub fn set_default_database_name(&mut self, default_database_name: String) {
        self.default_database_name = default_database_name;
    }
    pub fn set_pwd_public_key(&mut self, pwd_public_key: String) {
        self.pwd_public_key = pwd_public_key;
    }
    pub fn set_enable_druid(&mut self, enable_druid: bool) {
        self.enable_druid = enable_druid;
    }
    pub fn new() -> Self {
        Self {
            address: String::new(),
            port: 0,
            username: String::new(),
            password: String::new(),
            default_database_name: String::new(),
            pwd_public_key: String::new(),
            enable_druid: false,
        }
    }
}