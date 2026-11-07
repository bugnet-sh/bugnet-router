-- Claims a transfer record for one stream, checking every budget a claim spends
-- KEYS[1] the record, KEYS[2] the placement's active set
-- ARGV now in ms, lease, lease length in ms, edge id, max claims, max active, byte slack, id
local raw = redis.call('HGETALL', KEYS[1])
if #raw == 0 then
	return {'unknown'}
end
local h = {}
for i = 1, #raw, 2 do
	h[raw[i]] = raw[i + 1]
end
local now = tonumber(ARGV[1])
if h.state == 'done' then
	return {'used'}
end
local claims = tonumber(h.claims)
if h.state == 'active' and tonumber(h.lease_until) > now then
	return {'in_use'}
end
if now >= tonumber(h.expires_at) and (claims == 0 or h.direction == 'upload') then
	return {'expired'}
end
if claims >= tonumber(ARGV[5]) or
	tonumber(h.bytes_sent) >= 2 * tonumber(h.size) + tonumber(ARGV[7]) then
	return {'spent'}
end
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now)
redis.call('ZREM', KEYS[2], ARGV[8])
if redis.call('ZCARD', KEYS[2]) >= tonumber(ARGV[6]) then
	return {'too_many'}
end
local leaseUntil = now + tonumber(ARGV[3])
redis.call('HSET', KEYS[1], 'state', 'active', 'lease', ARGV[2], 'lease_until', leaseUntil,
	'claims', claims + 1, 'edge', ARGV[4])
if redis.call('PEXPIRETIME', KEYS[1]) < leaseUntil then
	redis.call('PEXPIREAT', KEYS[1], leaseUntil)
end
redis.call('ZADD', KEYS[2], leaseUntil, ARGV[8])
if redis.call('PEXPIRETIME', KEYS[2]) < leaseUntil then
	redis.call('PEXPIREAT', KEYS[2], leaseUntil)
end
local out = {'ok'}
for _, v in ipairs(redis.call('HGETALL', KEYS[1])) do
	out[#out + 1] = v
end
return out
