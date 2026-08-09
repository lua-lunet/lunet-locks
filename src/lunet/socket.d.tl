local record socket
    listen: function(string, string, integer): (any, string | nil)
    accept: function(any): (any, string | nil)
    read: function(any): (string | nil, string | nil)
    -- Lunet yields and resumes with nil on success or one error string.
    write: function(any, string): string | nil
    close: function(any)
end

return socket
