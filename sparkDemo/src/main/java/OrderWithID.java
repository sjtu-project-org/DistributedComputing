import java.io.Serializable;

public class OrderWithID implements Serializable {

    private static final long serialVersionUID = 1L;

    private String order_id;
    private Order order;

    public String getOrder_id() {
        return order_id;
    }

    public void setOrder_id(String order_id) {
        this.order_id = order_id;
    }

    public Order getOrder() {
        return order;
    }

    public void setOrder(Order order) {
        this.order = order;
    }

}
