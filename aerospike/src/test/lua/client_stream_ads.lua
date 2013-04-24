function stream_ads_2(s)

    local function mapper(rec)
        local key = tostring(rec['advertiser']) .. '/' .. tostring(rec['campaign']) .. '/' .. tostring(rec['line_item'])

        m = map{}
        m['advertiser'] = rec['advertiser']
        m['campain'] = rec['campaign']
        m['line_item'] = rec['line_item']
        m['spend_sum'] = rec['spend'] or 0
        m['spend_max'] = rec['spend'] or 0
        m['spend_num'] = 1

        m2 = map{}
        m2[key] = m
        return m2

    end

    local function merger(val1, val2)

        local sum = val1['spend_sum'] + val2['spend_sum']
        local max = val1['spend_max']
        local num = val1['spend_num'] + val2['spend_num']
        
        if val2['spend_max'] > max then
            max = val2['spend_max']
        end

        m = map{}
        m['advertiser'] = val1['advertiser']
        m['campaign']    = val1['campaign']
        m['line_item'] = val1['line_item']
        m['spend_sum'] = sum
        m['spend_max'] = max
        m['spend_num'] = num

        return m

    end

    local function reducer(map1, map2)
        return map.merge(map1, map2, merger)
    end

    return s : map(mapper) : reduce(reducer)
end




function stream_ads_3(s)

    local function agg(m, rec)
        local key = tostring(rec['advertiser']) .. '/' .. tostring(rec['campaign']) .. '/' .. tostring(rec['line_item'])
        local obj = m[key]

        if not obj then
            obj = map {
                advertiser  = rec['advertiser'],
                campaign    = rec['campaign'],
                line_item   = rec['line_item'],
                spend_sum   = rec['spend'] or 0,
                spend_max   = rec['spend'] or 0,
                spend_num   = 1
            }
        else
            obj['spend_sum'] = obj['spend_sum'] + rec['spend']
            if rec['spend'] > obj['spend_max']  then
                obj['spend_max'] = rec['spend']
            end
            obj['spend_num'] = obj['spend_num'] + 1
        end

        m[key] = obj

        return m
    end

    local function merge(o1, o2)

        local sum = o1['spend_sum'] + o2['spend_sum']
        local max = o1['spend_max']
        local num = o1['spend_num'] + o2['spend_num']
        
        if o2['spend_max'] > max then
            max = o2['spend_max']
        end

        return map {
            advertiser  = o1['advertiser'],
            campaign    = o1['campaign'],
            line_item   = o1['line_item'],
            spend_sum   = sum,
            spend_max   = max,
            spend_num   = num
        }
    end

    local function red(m1, m2)
        return map.merge(m1, m2, merge)
    end

    return s : aggregate(map(), agg) : reduce(red)
end



function stream_ads_4(s)

    local function agg(m, rec)

        local a = m[rec['advertiser']]  or map()
        local c = a[rec['campaign']]    or map()
        local l = c[rec['line_item']]

        if not l then
            l = map {
                spend_sum   = rec['spend'] or 0,
                spend_max   = rec['spend'] or 0,
                spend_num   = 1
            }
        else
            l['spend_sum'] = l['spend_sum'] + rec['spend']
            if rec['spend'] > l['spend_max']  then
                l['spend_max'] = rec['spend']
            end
            l['spend_num'] = l['spend_num'] + 1
        end

        c[rec['line_item']]   = l
        a[rec['campaign']]    = c
        m[rec['advertiser']]  = a

        return m
    end

    local function merge_line_item(l1, l2)

        local sum = l1['spend_sum'] + l2['spend_sum']
        local max = l1['spend_max']
        local num = l1['spend_num'] + l2['spend_num']
        
        if l2['spend_max'] > max then
            max = l2['spend_max']
        end

        return map {
            spend_sum   = sum,
            spend_max   = max,
            spend_num   = num
        }
    end

    local function merge_campaign(c1, c2)
        return map.merge(c1, c2, merge_line_item);
    end

    local function merge_advertiser(a1, a2)
        return map.merge(a1, a2, merge_campaign);
    end

    local function red(m1, m2)
        return map.merge(m1, m2, merge_advertiser)
    end

    return s : aggregate(map(), agg) : reduce(red)
end
