module.exports = {
  apps: [
    {
      name: 'hermes-wechaty',
      script: 'index.js',
      cwd: '/home/ubuntu/bess-platform/apps/hermes-wechaty',
      env: {
        HERMES_INBOUND_URL: 'http://HERMES_ECS_PRIVATE_IP:8000/hermes/inbound/wechat',
        BRIDGE_PORT: '3000',
        QR_PORT: '3001',
      },
      restart_delay: 5000,
      max_restarts: 10,
    },
  ],
};
