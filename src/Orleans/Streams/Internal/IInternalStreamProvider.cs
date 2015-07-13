namespace Orleans.Streams
{
    internal interface IInternalStreamProvider : IStreamProviderImpl
    {
        IStreamProviderRuntime StreamProviderRuntime { get; }
        IAsyncBatchObserver<T> GetProducerInterface<T>(IAsyncStream<T> streamId);
        IInternalAsyncObservable<T> GetConsumerInterface<T>(IAsyncStream<T> streamId);
    }
}
