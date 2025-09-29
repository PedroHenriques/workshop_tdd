namespace Consumer.Models;

public class OutboundKey { }

public class OutboundValue
{
  public required Guid Id { get; set; }
  public required int Type { get; set; }
  public required DateTime Timestamp { get; set; }
}