function file_exists(file, a)
    if aerospike:exists(file) then
        info("file exists!")
        return 1
    else
        info("file doesn't exist!")
        return 0
    end
end