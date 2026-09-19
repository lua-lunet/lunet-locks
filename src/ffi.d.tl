local record ffi
    os: string
    arch: string
    cdef: function(string)
    load: function(string): any
    new: function(string, ...: any): any
    gc: function(any, any): any
    string: function(any, ?integer): string
    cast: function(string, any): any
    -- The FFI C namespace holds every cdef'd symbol; calls through it
    -- are C calls the checker cannot see into.
    C: any
end

return ffi
