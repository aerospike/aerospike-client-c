local ACTION = 1
local TIMESTAMP = 2

-- -----------------------------------------------------------------------------
--
--	Read the information about each campaign requested
--
--	@param record 		The record to apply this function on.
--	@param ..			The campaigns to read for the user
--	
--	@return 0 on success, otherwise an error occurred
--
-- -----------------------------------------------------------------------------
function get_campaign(record, ...)

	-- contains the information for each campaign requested
	local result = map();

	-- iterate over each argument, which should be a campaign id
	for i=1, select("#", ...) do
		local campaign = select(i,...)
		if campaign then
			-- Convert the campaign to a string because the bin
			-- name is based on the campaign id, and bin names
			-- can only be strings
			local bin = "camp_" .. tostring(campaign)

			-- lua-ism: similar to ternary expression
			-- 		A or B
			-- If A is `false` or `nil`, then return B
			result[campaign] = record[bin] or list()
		end
	end

	return result
end

-- -----------------------------------------------------------------------------
--
--	Store in the record a bin called 'camp_{campaign}', where {campaign} is 
--	the campaign parameter.
--
--	Each campaign bin contains a [action, timestamp].
--
--	@param record 		The record to apply this function on.
--	@param campaign 	The campaign the action belongs to
--	@param action		The action being logged
--	@param timestamp	The timestamp of the action
--	
--	@return 0 on success, otherwise an error occurred
--
-- -----------------------------------------------------------------------------
function put_behavior(record, campaign, action, timestamp)

	-- precondition check
	-- lua-ism: similar to NULL check in C
	--		Lua:	not nil	=> true
	--		C: 		! NULL	=> true
	if not campaign then
		error("Missing campaign parameter.")
		return 101
	elseif not action then
		error("Missing action parameter.")
		return 102
	elseif not timestamp then
		error("Missing timestamp parameter")
		return 103
	end

	-- A bin per campaign
	local bin = "camp_" .. tostring(campaign)

	-- Get the bin's current value
	local values = record[bin]

	-- Process the parameters then update the bin's value
	if not values then
		-- the bin was not previously set 
		values = list { action, timestamp }
	else
		if action == 'click' then
			values = list{ action, timestamp }
		elseif timestamp > values[TIMESTAMP] then
			values = list{ action, timestamp }
		else
			-- unhandled
		end
	end

	-- Set the bin's value
	record[bin] = values

	-- Create or update the record
	if (aerospike:exists(record)) then
		return aerospike:update(record);
	else
		return aerospike:create(record);
	end
end
