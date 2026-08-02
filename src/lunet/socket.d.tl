local record socket
   listen: function(string, string, integer): (userdata, string)
   accept: function(userdata): (userdata, string)
   read: function(userdata): string
   write: function(userdata, string)
   close: function(userdata)
end

return socket
