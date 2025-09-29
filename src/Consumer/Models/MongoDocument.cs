namespace Consumer.Models;

public class MongoDocument
{
  public required Metadata Metadata { get; set; }
  public required Message Message { get; set; }
}

public class Metadata
{
  public DateTime Timestamp { get; set; }
  public Guid Id { get; set; }
}

public class Message
{
  public required OutboundKey Key { get; set; }
  public required OutboundValue Value { get; set; }
}