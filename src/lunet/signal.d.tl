-- One-shot: the first matching signal resumes the waiter and tears the
-- watcher down; it does NOT terminate the process (e.g. the first SIGTERM
-- is delivered, not fatal). Exit from the handler if you want to stop.
local record signal
    wait: function(name: string): string, string
end

return signal
