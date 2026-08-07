local record ffi
    os: string
    cdef: function(string)
    load: function(string): any
    new: function(string, ...: any): any
    gc: function(any, any): any
    string: function(any, integer): string
end

return ffi
