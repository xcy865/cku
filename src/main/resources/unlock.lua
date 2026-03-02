-- 定义KEYS和ARGV参数（Redis Lua脚本规范）
-- KEYS[1] = 拼接了KEY_PREFIX的完整锁key（例如：lock:order）
-- ARGV[1] = 拼接了ID_PREFIX的线程标识（例如：thread:1）

-- 1. 获取Redis中存储的锁标识
local lock_id = redis.call('get', KEYS[1])

-- 2. 判断标识是否存在且与当前线程标识一致
if lock_id == ARGV[1] then
    -- 3. 标识一致，释放锁（删除key）
    redis.call('del', KEYS[1])
    -- 返回1表示释放成功
    return 1
else
    -- 返回0表示释放失败（锁不属于当前线程）
    return 0
end
