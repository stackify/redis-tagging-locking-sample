--[[
Loops through the list of tags and cleans out any references to
expired keys. If all keys are cleaned from a tag, removes the tag.
--]]
local tagList = {}
local tagsRemoved = 0
local keysCleaned = 0
if(#tagList == 0) then
	tagList = redis.call('keys', '*:tag:*')
end
for _, tagName in pairs(tagList) do
	local tagType = redis.call('type', tagName)
	if(tagType['ok'] == 'set') then
	
		local tagKeys = redis.call('smembers', tagName)
	
		local tagActive = 0
		local deadKeys = {}
		for _, key in pairs(tagKeys) do 
			local keyActive = redis.call('exists', key)
			if(keyActive == 0) then
				table.insert(deadKeys, key)
			end
		end
	
		if(#deadKeys > 0) then 
			redis.call('srem', tagName, unpack(deadKeys))
		end
	
		if(#deadKeys == #tagKeys) then
			redis.call('del', tagName)
			tagsRemoved = tagsRemoved + 1
		end
		keysCleaned = keysCleaned + #deadKeys
	end
end
return keysCleaned