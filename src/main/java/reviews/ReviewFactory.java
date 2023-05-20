package reviews;



public class ReviewFactory {

    private String title;

    public ReviewFactory(String title) {
        this.title = title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Review make(long userId, int value, long ts){
        return new Review(userId, title, ts, value);
    }

    public String getTitleName() {
        return title;
    }
}
