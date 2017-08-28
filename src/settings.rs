use config::{ConfigError, Config, File};
use hostname::get_hostname;

#[derive(Debug, Deserialize)]
pub struct Database {
    pub url: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Topic {
    pub name: String,
    pub compacted: Option<bool>,
    pub retention: Option<u64>
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    listen: Option<String>,
    hostname: Option<String>,
    pub cleanup: Option<u64>,
    pub threads: Option<usize>,
    pub database: Database,
    pub topics: Vec<Topic>
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        let mut s = Config::new();
        s.merge(File::with_name("config/unclek").required(false))?;
        s.try_into()
    }
    
    pub fn listen(&self) -> String {
		match self.listen {
			Some(ref s) => s.to_string(),
			None => String::from("0.0.0.0:9092")
		}
	}
	
	pub fn get_hostname(&self) -> String {
		match self.hostname {
			Some(ref v) => v.to_string(),
			None        => get_hostname().expect("Failed to get localhost's hostname")
		}
	}
}
