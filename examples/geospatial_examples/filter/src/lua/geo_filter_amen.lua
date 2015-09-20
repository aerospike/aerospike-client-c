local function map_loc(rec)
  -- info('%s %s', rec.geofilteramen, tostring(rec.geofilterloc))
  return rec.geofilterloc
end

function match_amen(stream, val)
  local function filter_amen(rec)
    return rec.geofilteramen == val
  end
  return stream : filter(filter_amen) : map(map_loc)
end
