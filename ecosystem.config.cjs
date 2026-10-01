/** PM2 config — `./start.sh` or `pm2 startOrRestart ecosystem.config.cjs` */
module.exports = {
  apps: [
    {
      name: "sn-monitor",
      cwd: "/root/draven/sn_monitor",
      script: ".venv/bin/python",
      args: "-m snmon",
      interpreter: "none",
      autorestart: true,
      restart_delay: 2000,
      max_restarts: 1000,
      kill_timeout: 3000,
      env: {
        PYTHONUNBUFFERED: "1",
        // keep any shell proxies out of the websocket / Discord connections
        HTTP_PROXY: "", HTTPS_PROXY: "", ALL_PROXY: "",
        http_proxy: "", https_proxy: "", all_proxy: "",
      },
    },
  ],
};
