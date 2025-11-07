#!/bin/sh

# Start the cron daemon in the background
cron

# Execute the main container command (passed as arguments to this script)
exec "$@"
