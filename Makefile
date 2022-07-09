install-service:
	install oda-bot.service $(HOME)/.config/systemd/user/oda-bot.service
	systemctl --user daemon-reload
	systemctl --user enable oda-bot.service 
	install settings.toml -m 644 -Dv ~/.config/oda/bot
	echo systemctl --user start oda-bot.service 
