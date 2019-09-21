package io.github.olivierlemasle.ca;

public class CaException extends RuntimeException {
  private static final long serialVersionUID = -9188923051885159431L;

  public CaException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public CaException(final String message) {
    super(message);
  }

  public CaException(final Throwable cause) {
    super(cause);
  }

}
