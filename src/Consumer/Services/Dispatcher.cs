namespace Consumer;

public interface IDispatcher
{
  public void Dispatch();
}

public class Dispatcher : IDispatcher
{
  public Dispatcher()
  {
  }

  public void Dispatch() { }
}