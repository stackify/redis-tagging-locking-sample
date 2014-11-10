--[[
For a cache item and associating one or more tags.
Given a cache key (KEYS[1]) and value (ARGV[1]), adds the
key-value pair to the cache and associates it to the given
list of tags (KEYS[2+]). If an expiration (ARGV[2]) is
supplied, the key-value pair will expire accordingly, and
any tags expire in ARGV[2] _or greater_ seconds.
--]]
local tagcount = 0
local cacheKey = KEYS[1]
local exp = ARGV[2]
local setValue = ARGV[1]
for i=2,#KEYS do
   	local tagTtl = redis.call('ttl', KEYS[i])
   	tagcount = tagcount + redis.call('sadd', KEYS[i], cacheKey)
   	redis.call('expire', KEYS[i], math.max(tagTtl, exp or 3600))
end
 
if(setValue) then
   	if(exp ~= nil) then
          	redis.call('setex', cacheKey, exp, setValue)
   	else
          	redis.call('set', cacheKey, setValue)
   	end
end
return tagcount