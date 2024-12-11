//! 实现Db结构体

use bincode;
use bytes::Bytes;
use serde::{ser::SerializeStruct, Deserialize, Serialize, Serializer};
use std::{
    collections::{BTreeSet, HashMap},
    fs::File,
    io::Read,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{broadcast, Notify},
    time::{self, Duration, Instant},
};
use tracing::debug;

/// # DatabaseWrapper 结构体
///
/// 封装一个Database实例
/// 当DatabaseWrapper实例被销毁时，会自动关闭Database实例
/// 为啥不直接为Database实现Drop traitn呢？因为Database能够在多线程中被共享（通过Arc）
/// 如果这样操作了，那可能当一个线程的Database实例被销毁时，其他线程还在使用这个实例，这样就会出现问题
#[derive(Debug)]
pub(crate) struct DatabaseWrapper {
    database: Database,
}

impl DatabaseWrapper {
    pub(crate) fn new() -> DatabaseWrapper {
        let mut database = Database::new();

        // 加载RDB文件
        database = if let Ok(db) = Database::load_from_rdb("rustis.rdb") {
            db
        } else {
            database
        };

        DatabaseWrapper { database }
    }

    /// # database() 函数
    ///
    /// 返回一个Database实例的Clone
    pub(crate) fn database(&self) -> Database {
        self.database.clone()
    }
}

impl Drop for DatabaseWrapper {
    fn drop(&mut self) {
        // 向database实例发送关闭信号
        self.database.shutdown_clean_task();
    }
}

/// # Database 结构体
#[derive(Debug, Clone)]
pub(crate) struct Database {
    /// shared需要在多线程中共享
    shared: Arc<Shared>,
}

impl Database {
    /// # new() 函数
    ///
    /// 创建一个新的Database实例，并运行一个后台人物去管理密钥的过期
    pub(crate) fn new() -> Self {
        let shared = Arc::new(Shared::new(
            Mutex::new(State::new(
                HashMap::new(),
                HashMap::new(),
                BTreeSet::new(),
                false,
            )),
            Notify::new(),
        ));

        // 开启一个后台任务，用来清除过期的密钥
        tokio::spawn(clean_expired_keys(shared.clone()));

        Self { shared }
    }

    /// # get() 函数
    ///
    /// 获取一个键的值
    pub(crate) fn get(&self, key: &str) -> Option<Bytes> {
        // 获取state锁
        let state = self.shared.state.lock().unwrap();
        state.entries.get(key).map(|entry| entry.data.clone())
    }

    /// # set() 函数
    ///
    /// 设置一个键的值
    pub(crate) fn set(&self, key: String, value: Bytes, expire: Option<Duration>) {
        // 获取state锁
        let mut state = self.shared.state.lock().unwrap();

        // 如果这个设置的键是快要过期的，需要通知后台任务更新状态
        let mut notify = false;

        let expire_at = expire.map(|duration| {
            // 将当前时间转换为UNIX时间戳
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let when = now + duration.as_secs();

            notify = state
                .next_expiration()
                .map(|expiration| expiration > when)
                .unwrap_or(true);

            when
        });

        // 将entry插入到entries中，prev是在插入新entry后返回的旧entry
        let prev = state
            .entries
            .insert(key.clone(), Entry::new(value, expire_at));

        // 去除旧的过期时间
        if let Some(prev) = prev {
            // 如果旧条目有过期时间，在expirations中去除掉
            if let Some(when) = prev.expires_at {
                // 清除过期的键
                state.expirations.remove(&(when, key.clone()));
            }
        }

        // 插入新的过期时间
        if let Some(when) = expire_at {
            state.expirations.insert((when, key));
        }

        drop(state);

        if notify {
            // 通知后台任务更新状态
            self.shared.notify_background_task.notify_one();
        }
    }

    /// # subscribe() 函数
    ///
    /// 返回一个Receiver，用于接收publish命令广播的值
    pub(crate) fn subscribe(&self, key: String) -> broadcast::Receiver<Bytes> {
        use std::collections::hash_map::Entry;

        // 获取state锁
        let mut state = self.shared.state.lock().unwrap();

        // 如果请求的channel没有，那就创建一个新的广播chennel并将其与键关联
        // 如果有，返回关联的receiver
        match state.pub_sub.entry(key) {
            Entry::Occupied(e) => e.get().subscribe(),
            Entry::Vacant(e) => {
                let (tx, rx) = broadcast::channel(1024);
                e.insert(tx);
                rx
            }
        }
    }

    /// # publish() 函数
    ///
    /// 将消息发布到channel，返回在channel上侦听的subscriber数量
    ///
    /// # 参数
    ///
    /// - channel: channel在pub_sub中的key
    /// - message：要发送的消息
    pub(crate) fn publish(&self, channel: &str, message: Bytes) -> usize {
        // 获取state锁
        let state = self.shared.state.lock().unwrap();

        state
            .pub_sub
            .get(channel)
            .map(|tx| tx.send(message).unwrap_or(0))
            .unwrap_or(0)
    }

    /// # shutdown_clean_task() 函数
    ///
    /// 指示后台任务关闭
    fn shutdown_clean_task(&self) {
        // 获取state锁
        let mut state = self.shared.state.lock().unwrap();
        state.shutdown = true;

        // 在通知后台任务之前，释放锁，减少锁竞争
        drop(state);

        // 后台任务会收到新的信号
        self.shared.notify_background_task.notify_one();
    }

    pub fn save_to_rdb(&self, file_path: &str) -> crate::Result<()> {
        let state = self.shared.state.lock().unwrap();

        let file = File::create(file_path)?;
        bincode::serialize_into(file, &state.entries)?;

        Ok(())
    }

    pub fn load_from_rdb(file_path: &str) -> crate::Result<Database> {
        let mut file = File::open(file_path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        let data: HashMap<String, Entry> = bincode::deserialize(&buffer)?;

        Ok(Database {
            shared: Arc::new(Shared::new(
                Mutex::new(State::new(data, HashMap::new(), BTreeSet::new(), false)),
                Notify::new(),
            )),
        })
    }
}

#[derive(Debug)]
struct Shared {
    /// 状态
    state: Mutex<State>,
    /// 通知后台任务处理过期Entry
    notify_background_task: Notify,
}

impl Shared {
    fn new(state: Mutex<State>, notify_background_task: Notify) -> Self {
        Self {
            state,
            notify_background_task,
        }
    }

    /// clean_expired_keys() 函数
    ///
    /// 清除所有过期的键并返回下一个密钥到期的instant，后台任务将一直休眠到这个时刻
    fn clean_expired_keys(&self) -> Option<u64> {
        // 获取state锁
        let mut state = self.state.lock().unwrap();

        // 如果数据库已经关闭，则返回None
        if state.shutdown {
            return None;
        }

        // rust的借用检查器无法通过MutexGuard来确定同时访问expirations和entries是安全的
        // 为了让借用检查器能够正常工作，需要获取一个真正的可变引用
        let state = &mut *state;

        // 获取当前时间
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // 从expirations中找到所有已经过期的键
        while let Some(&(when, ref key)) = state.expirations.iter().next() {
            // 如果返回的时间大于now，直接返回下一个键过期的时间
            if when > now {
                return Some(when);
            }

            // 如果返回的时间小于now，那么从entries中删除这个键
            state.entries.remove(key);
            state.expirations.remove(&(when, key.clone()));
        }

        None
    }

    /// is_shutdown() 函数
    ///
    /// 如果数据库已经关闭，则返回true
    fn is_shutdown(&self) -> bool {
        self.state.lock().unwrap().shutdown
    }
}

#[derive(Debug)]
struct State {
    /// 键值数据
    entries: HashMap<String, Entry>,
    /// 发布/订阅的键空间，redis中为其单独使用一个键值空间
    pub_sub: HashMap<String, broadcast::Sender<Bytes>>,
    /// 维护keys的过期时间
    expirations: BTreeSet<(u64, String)>,
    /// db实例的开启/关闭状态
    shutdown: bool,
}

impl State {
    fn new(
        entries: HashMap<String, Entry>,
        pub_sub: HashMap<String, broadcast::Sender<Bytes>>,
        expirations: BTreeSet<(u64, String)>,
        shutdown: bool,
    ) -> Self {
        Self {
            entries,
            pub_sub,
            expirations,
            shutdown,
        }
    }

    /// next_expiration() 函数
    ///
    /// 返回下一个密钥到期的时间
    fn next_expiration(&self) -> Option<u64> {
        self.expirations
            .iter()
            .next()
            .map(|expiration| expiration.0)
    }
}

#[derive(Debug)]
struct Entry {
    /// 存储数据
    data: Bytes,
    /// 数据的过期时间，使用UNIX时间戳（自1970年1月1日以来的秒数）表示
    expires_at: Option<u64>,
}

impl Entry {
    fn new(data: Bytes, expires_at: Option<u64>) -> Self {
        Self { data, expires_at }
    }
}

// 为Entry实现Serialize trait
impl Serialize for Entry {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // 先将Bytes类型转换为Vec<u8>类型（可序列化）
        let data = self.data.clone().to_vec();
        let expires_at = self.expires_at;
        let mut state = serializer.serialize_struct("Entry", 2)?;
        state.serialize_field("data", &data)?;
        state.serialize_field("expires_at", &expires_at)?;
        state.end()
    }
}

// 为Entry实现Deserialize trait
impl<'de> Deserialize<'de> for Entry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct EntryData {
            data: Vec<u8>,
            expires_at: Option<u64>,
        }

        let entry_data = EntryData::deserialize(deserializer)?;
        Ok(Self {
            data: Bytes::from(entry_data.data),
            expires_at: entry_data.expires_at,
        })
    }
}

/// # clean_expired_keys() 函数
///
/// 后台任务执行的例程
/// - 等待通知，收到通知后，从shared中清除过期的键
/// - 如果数据库已经关闭，则退出
async fn clean_expired_keys(shared: Arc<Shared>) {
    // 如果shutdown标识为true，则退出
    while !shared.is_shutdown() {
        // 如果存在过期的键，则清除，否则继续等待通知
        if let Some(when) = shared.clean_expired_keys() {
            // 将 u64 转换为 Instant
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let duration_until_expiration = when.saturating_sub(now);
            let when_instant = Instant::now() + Duration::from_secs(duration_until_expiration);

            // 等待直到下一个密钥过期或等待通知
            tokio::select! {
                _ = time::sleep_until(when_instant) => {},
                // 如果收到通知，那么它必须重新加载状态，因为新密钥被设置为提前过期
                _ = shared.notify_background_task.notified() => {},
            }
        } else {
            // 这里没有过期的键，等待通知
            shared.notify_background_task.notified().await;
        }
    }

    debug!("清除过期键的后台任务已经被关闭");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    /// 测试简单的set和get
    #[tokio::test]
    async fn test_get_and_set() {
        let db = Database::new();

        // 设置一个键值对
        let key = "test_key".to_string();
        let value = Bytes::from("test_value");
        // 调用set方法
        db.set(key.clone(), value.clone(), None);

        // 调用get方法
        let result = db.get(&key);

        assert_eq!(result, Some(value));
    }

    /// 测试获取不存在的键
    #[tokio::test]
    async fn test_get_nonexistent_key() {
        let db = Database::new();
        let ret = db.get("nonexistent_key");
        assert_eq!(ret, None);
    }

    /// 测试pub/sub
    #[tokio::test]
    async fn test_subscribe_and_publish() {
        let db = Database::new();

        let channel = "test_channel".to_string();
        let message = Bytes::from("Hello, channel");

        // 订阅channel
        let mut subscriber1 = db.subscribe(channel.clone());
        let mut subscriber2 = db.subscribe(channel.clone());

        // 发布消息到channle
        let subscribers_number = db.publish(&channel, message.clone());

        // 检查subscriber数量
        assert_eq!(subscribers_number, 2);

        // 验证第一个subcriber收到的消息
        let received1 = subscriber1.recv().await.unwrap();
        assert_eq!(received1, message);

        // 验证第二个subscriber收到的消息
        let received2 = subscriber2.recv().await.unwrap();
        assert_eq!(received2, message);
    }

    /// 测试发布到没有订阅者的channel
    #[tokio::test]
    async fn test_publish_without_subscribers() {
        let db = Database::new();

        let channel = "test_channel".to_string();
        let message = Bytes::from("No subscribers");

        let subscribers_number = db.publish(&channel, message.clone());

        assert_eq!(subscribers_number, 0);
    }

    /// 测试保存rdb和加载rdb
    #[tokio::test]
    async fn test_rdb_save_and_load() {
        let db = Database::new();

        // 添加测试数据
        db.set("test_key1".to_string(), Bytes::from("test_value1"), None);
        db.set("test_key2".to_string(), Bytes::from("test_value2"), None);

        // 保存到rdb
        let file_path = "test.rdb";
        db.save_to_rdb(file_path).expect("Failed to save RDB");

        // 从rdb加载
        let loaded_db = Database::load_from_rdb(file_path).expect("Failed to load RDB");

        // 校验数据
        assert_eq!(db.get("test_key1"), loaded_db.get("test_key1"));
        assert_eq!(db.get("test_key2"), loaded_db.get("test_key2"));

        // 删除rdb文件
        fs::remove_file(file_path).expect("Failed to remove RDB file");
    }
}
