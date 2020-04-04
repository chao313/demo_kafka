package demo.kafka.controller.admin.test;

public enum Bootstrap {
    MY("10.202.16.136:9092"), WIND("10.200.126.163:9092"),HONE("192.168.0.105:9092");
    public static final String HONE_IP = "192.168.0.105:9092";

    private String ip;

    Bootstrap(String ip) {
        this.ip = ip;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }
}
