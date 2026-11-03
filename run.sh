#!/usr/bin/env bash
# DDD + Fluss demo 的一键脚本。
set -euo pipefail
cd "$(dirname "$0")"

JAR="target/ddd-fluss-demo-1.0.0.jar"
# Arrow 15 在 JDK 17+ 上需要显式开放 java.nio 的反射访问，否则日志表读写会失败。
JAVA_OPTS=(
  --add-opens=java.base/java.nio=ALL-UNNAMED
  -Dio.netty.tryReflectionSetAccessible=true
)

usage() {
  cat <<'EOF'
用法: ./run.sh <命令>

  up        启动本地 Fluss 集群（ZK + coordinator:9123 + tablet:9124）
  down      停止并删除集群及数据卷
  build     编译打包
  test      跑领域单元测试（不依赖 Fluss）
  smoke     只验证 Fluss 连通性和读写（建表/追加/扫描/lookup）
  verify    一键验证：跑完所有场景和缺陷探针后退出
  server    以 Web 模式启动（REST 接口在 8080）
  logs      跟随集群日志
EOF
}

wait_for_fluss() {
  echo "等待 Fluss 就绪 (localhost:9123) ..."
  for _ in $(seq 1 90); do
    if nc -z localhost 9123 2>/dev/null; then
      echo "Fluss 已就绪"
      return 0
    fi
    sleep 1
  done
  echo "Fluss 在 90 秒内未就绪，查看日志: ./run.sh logs" >&2
  return 1
}

case "${1:-help}" in
  up)
    docker compose up -d
    wait_for_fluss
    ;;
  down)
    docker compose down -v
    ;;
  build)
    mvn -q -B package -DskipTests
    ;;
  test)
    mvn -B test
    ;;
  smoke)
    mvn -q -B package -DskipTests
    java "${JAVA_OPTS[@]}" -cp "$JAR" com.example.orderdemo.SmokeCheck
    ;;
  verify)
    mvn -q -B package -DskipTests
    java "${JAVA_OPTS[@]}" -jar "$JAR" \
      --demo.verify=true \
      --spring.main.web-application-type=none
    ;;
  server)
    mvn -q -B package -DskipTests
    java "${JAVA_OPTS[@]}" -jar "$JAR"
    ;;
  logs)
    docker compose logs -f
    ;;
  *)
    usage
    exit 1
    ;;
esac
