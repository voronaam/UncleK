use config::{ConfigError, Config, File};

#[derive(Debug, Deserialize)]
pub struct Database {
    pub url: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Topic {
    pub name: String,
    pub compacted: Option<bool>
}

#[derive(Debug, Deserialize)]
pub struct Settings {
    listen: Option<String>,
    pub threads: Option<usize>,
    pub database: Database,
    pub topic: Vec<Topic>
}

impl Settings {
    pub fn new() -> Result<Self, ConfigError> {
        let mut s = Config::new();
        s.merge(File::with_name("unclek").required(false))?;
        s.try_into()
    }
    
    pub fn listen(&self) -> String {
		match self.listen {
			Some(ref s) => s.to_string(),
			None => String::from("0.0.0.0:9092")
		}
	}
}
