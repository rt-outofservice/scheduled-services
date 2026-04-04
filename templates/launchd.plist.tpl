<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{{LABEL}}</string>
    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string>-c</string>
        <string>{{COMMAND}}</string>
    </array>
    <key>WorkingDirectory</key>
    <string>{{WORKING_DIR}}</string>
    <key>StartCalendarInterval</key>
    {{SCHEDULE}}
    <key>StandardOutPath</key>
    <string>{{LOG_DIR}}/launchd-stdout.log</string>
    <key>StandardErrorPath</key>
    <string>{{LOG_DIR}}/launchd-stderr.log</string>
</dict>
</plist>
