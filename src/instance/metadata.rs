pub struct EntryPosition {
    timestamp: Option<u64>,
    eventidentity_segment: Option<i32>,
    eventidentity_split: Option<char>,
    included: Option<bool>,
    journal_name: Option<String>,
    position: Option<u32>,
    server_id: Option<u32>,
    gtid: Option<String>,
}


impl Clone for EntryPosition {
    fn clone(&self) -> Self {
        Self {
            timestamp: self.timestamp,
            eventidentity_segment: self.eventidentity_segment,
            eventidentity_split: self.eventidentity_split,
            included: self.included,
            journal_name: self.journal_name.clone(),
            position: self.position,
            server_id: self.server_id,
            gtid: self.gtid.clone()
        }
    }
}
impl EntryPosition {
    pub fn new() -> Self {
        Self {
            timestamp: Option::None,
            eventidentity_segment: Option::None,
            eventidentity_split: Option::None,
            included: Option::None,
            journal_name: Option::None,
            position: Option::None,
            server_id: Option::None,
            gtid: Option::None,
        }
    }

    fn _from_timestamp(timestamp: u64) -> EntryPosition {
        EntryPosition {
            timestamp: Option::Some(timestamp),
            eventidentity_segment: Option::None,
            eventidentity_split: Option::None,
            included: Option::None,
            journal_name: Option::None,
            position: Option::None,
            server_id: Option::None,
            gtid: Option::None,
        }
    }

    pub fn from_position(journal_name: String, position: u32) -> EntryPosition {
        EntryPosition {
            timestamp: Option::None,
            eventidentity_segment: Option::None,
            eventidentity_split: Option::None,
            included: Option::None,
            journal_name: Option::Some(journal_name),
            position: Option::Some(position),
            server_id: Option::None,
            gtid: Option::None,
        }
    }

    fn _from_journal_name_pos_timestamp(journal_name: String, position: u32, timestamp: u64) -> EntryPosition {
        EntryPosition {
            timestamp: Option::Some(timestamp),
            eventidentity_segment: Option::None,
            eventidentity_split: Option::None,
            included: Option::None,
            journal_name: Option::Some(journal_name),
            position: Option::Some(position),
            server_id: Option::None,
            gtid: Option::None,
        }
    }

    fn _from_server_id(journal_name: String, position: u32, timestamp: u64, server_id: u32) -> EntryPosition {
        EntryPosition {
            timestamp: Option::Some(timestamp),
            eventidentity_segment: Option::None,
            eventidentity_split: Option::None,
            included: Option::None,
            journal_name: Option::Some(journal_name),
            position: Option::Some(position),
            server_id: Option::Some(server_id),
            gtid: Option::None,
        }
    }


    pub fn timestamp(&self) -> Option<u64> {
        self.timestamp
    }
    pub fn eventidentity_segment(&self) -> Option<i32> {
        self.eventidentity_segment
    }
    pub fn eventidentity_split(&self) -> Option<char> {
        self.eventidentity_split
    }
    pub fn included(&self) -> Option<bool> {
        self.included
    }
    pub fn journal_name(&self) -> &Option<String> {
        &self.journal_name
    }
    pub fn position(&self) -> Option<u32> {
        self.position
    }
    pub fn server_id(&self) -> Option<u32> {
        self.server_id
    }
    pub fn gtid(&self) -> &Option<String> {
        &self.gtid
    }


    pub fn set_timestamp(&mut self, timestamp: Option<u64>) {
        self.timestamp = timestamp;
    }
    pub fn set_eventidentity_segment(&mut self, eventidentity_segment: Option<i32>) {
        self.eventidentity_segment = eventidentity_segment;
    }
    pub fn set_eventidentity_split(&mut self, eventidentity_split: Option<char>) {
        self.eventidentity_split = eventidentity_split;
    }
    pub fn set_included(&mut self, included: Option<bool>) {
        self.included = included;
    }
    pub fn set_journal_name(&mut self, journal_name: Option<String>) {
        self.journal_name = journal_name;
    }
    pub fn set_position(&mut self, position: Option<u32>) {
        self.position = position;
    }
    pub fn set_server_id(&mut self, server_id: Option<u32>) {
        self.server_id = server_id;
    }
    pub fn set_gtid(&mut self, gtid: Option<String>) {
        self.gtid = gtid;
    }
}