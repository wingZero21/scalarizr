upstream backend {
        ip_hash;

${upstream_hosts}
}
