-- KEYS[1]: scheduled_jobs (ZSET)
-- KEYS[2]: ready_jobs (Stream)
-- ARGV[1]: current_unix_timestamp
-- ARGV[2]: batch_limit

local due_jobs = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
if #due_jobs == 0 then
    return 0
end

for _, job_id in ipairs(due_jobs) do
    redis.call('ZREM', KEYS[1], job_id)
    redis.call('XADD', KEYS[2], '*', 'job_id', job_id)
end

return #due_jobs