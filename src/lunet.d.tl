local record lunet
    sleep: function(integer)
    spawn: function(function())
    -- Request a deliberate stop of the event loop. Repeat calls are safe.
    stop: function()
    -- Register the post-drain hook: called exactly once at the drain point,
    -- after in-memory state is final and before any handle is closed. Only
    -- synchronous work may happen in the callback. Replaces any previous one.
    on_stop: function(fn: function)
end

return lunet
