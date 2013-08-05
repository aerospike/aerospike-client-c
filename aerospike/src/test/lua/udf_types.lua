function get_nil()
    return nil
end

function get_true()
    return true
end

function get_false()
    return false
end

function get_string()
    return "abc"
end

function get_integer()
    return 123
end

function get_map()
    return map {
        a = 1,
        b = 2,
        c = 3
    }
end

function get_list()
    return list {1,2,3}
end