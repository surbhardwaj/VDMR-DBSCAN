package KD_pack;

public class KeySizeException extends KDException {

    protected KeySizeException() {
	super("Key size mismatch");
    }
    
    // arbitrary; every serializable class has to have one of these
    public static final long serialVersionUID = 2L;
    
}