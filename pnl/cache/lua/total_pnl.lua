-- Atomic per-symbol write + portfolio-total recompute (design doc Sec.10).
--
-- Per-symbol PnL fields never collide with each other, but the aggregate
-- "total" field is a read-sum-write: two symbols updating concurrently could
-- both read a stale total and clobber each other. A Lua script runs
-- atomically on Redis, so this is the one place in the design that needs one.
--
-- KEYS[1] = the client's hash, e.g. "client:{client_id}"
-- ARGV[1] = (optional) symbol field to change first
-- ARGV[2] = (optional) its JSON blob, e.g. {"pnl": "123.45", ...}
--           a symbol with an empty blob removes that field instead
-- With no ARGV the script only recomputes "total".
--
-- Money is summed exactly: Redis Lua numbers are doubles, so each "pnl"
-- string is parsed into integer units of 10^-4 (exact up to ~9e11), summed,
-- and formatted back. Digits past the 4th decimal are rounded half away
-- from zero.
--
-- Returns the new total as a decimal string.

local SCALE = 10000
local SCALE_DIGITS = 4

local function to_units(s)
  if type(s) == 'number' then
    s = string.format('%.' .. SCALE_DIGITS + 1 .. 'f', s)
  end
  local sign = 1
  if string.sub(s, 1, 1) == '-' then
    sign = -1
    s = string.sub(s, 2)
  end
  local int_part, frac_part = string.match(s, '^(%d*)%.?(%d*)$')
  if int_part == nil then
    error('unparseable pnl value: ' .. s)
  end
  if int_part == '' then int_part = '0' end
  local padded = frac_part .. string.rep('0', SCALE_DIGITS + 1)
  local units = tonumber(int_part) * SCALE + tonumber(string.sub(padded, 1, SCALE_DIGITS))
  if tonumber(string.sub(padded, SCALE_DIGITS + 1, SCALE_DIGITS + 1)) >= 5 then
    units = units + 1
  end
  return sign * units
end

local function from_units(u)
  local sign = ''
  if u < 0 then
    sign = '-'
    u = -u
  end
  local int_part = math.floor(u / SCALE)
  local frac_part = u - int_part * SCALE
  return sign .. string.format('%d.%0' .. SCALE_DIGITS .. 'd', int_part, frac_part)
end

local symbol = ARGV[1]
local blob = ARGV[2]
if symbol ~= nil and symbol ~= '' then
  if blob ~= nil and blob ~= '' then
    redis.call('HSET', KEYS[1], symbol, blob)
  else
    redis.call('HDEL', KEYS[1], symbol)
  end
end

local fields = redis.call('HGETALL', KEYS[1])
local total = 0
for i = 1, #fields, 2 do
  if fields[i] ~= 'total' then
    local decoded = cjson.decode(fields[i + 1])
    if type(decoded) == 'table' and decoded['pnl'] ~= nil then
      total = total + to_units(decoded['pnl'])
    end
  end
end

local out = from_units(total)
redis.call('HSET', KEYS[1], 'total', out)
return out
