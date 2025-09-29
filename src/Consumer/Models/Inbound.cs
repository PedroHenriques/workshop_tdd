namespace Consumer.Models;

public class InboundKey
{
  public required string Id { get; set; }
}

public class InboundValue
{
  public required Guid Id { get; set; }
  public string? Desc { get; set; }
  public required int Type { get; set; }
  public required double Price { get; set; }
}