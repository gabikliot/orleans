using System;
using System.Threading.Tasks;
using Orleans.CodeGeneration;
using Orleans.Runtime.Scheduler;

namespace Orleans.Runtime
{
    internal abstract class SystemTarget : ISystemTarget, ISystemTargetBase, IInvokable, IStreamable
    {
        private IGrainMethodInvoker lastInvoker;
        private ExtensionInvoker extensionInvoker;
        private readonly SchedulingContext schedulingContext;
        private Message running;
        
        protected SystemTarget(GrainId grainId, SiloAddress silo) 
            : this(grainId, silo, false)
        {
        }

        protected SystemTarget(GrainId grainId, SiloAddress silo, bool lowPriority)
        {
            GrainId = grainId;
            Silo = silo;
            ActivationId = ActivationId.GetSystemActivation(grainId, silo);
            schedulingContext = new SchedulingContext(this, lowPriority);
        }

        public SiloAddress Silo { get; private set; }
        public GrainId GrainId { get; private set; }
        public ActivationId ActivationId { get; set; }

        internal SchedulingContext SchedulingContext { get { return schedulingContext; } }

        #region Method invocation

        public IGrainMethodInvoker GetInvoker(int interfaceId, string genericGrainType = null)
        {
            return ActivationData.GetInvoker(ref lastInvoker, ref extensionInvoker, interfaceId, genericGrainType);
        }

        public bool TryAddExtension(IGrainExtensionMethodInvoker invoker, IGrainExtension extension)
        {
            if(extensionInvoker == null)
                extensionInvoker = new ExtensionInvoker();

            return extensionInvoker.TryAddExtension(invoker, extension);
        }

        public void RemoveExtension(IGrainExtension extension)
        {
            if (extensionInvoker != null)
            {
                if (extensionInvoker.Remove(extension))
                    extensionInvoker = null;
            }
            else
                throw new InvalidOperationException("Grain extensions not installed.");
        }

        public bool TryGetExtensionHandler(Type extensionType, out IGrainExtension result)
        {
            result = null;
            return extensionInvoker != null && extensionInvoker.TryGetExtensionHandler(extensionType, out result);
        }

        #endregion

        private Streams.StreamDirectory streamDirectory;
        public Streams.StreamDirectory GetStreamDirectory()
        {
            return streamDirectory ?? (streamDirectory = new Streams.StreamDirectory());
        }

        public bool IsUsingStreams
        {
            get { return streamDirectory != null; }
        }

        public async Task DeactivateStreamResources()
        {
            if (streamDirectory == null) return; // No streams - Nothing to do.
            if (extensionInvoker == null) return; // No installed extensions - Nothing to do.
            await streamDirectory.Cleanup();
        }

        public void HandleNewRequest(Message request)
        {
            running = request;
            InsideRuntimeClient.Current.Invoke(this, this, request).Ignore();
        }

        public void HandleResponse(Message response)
        {
            running = response;
            InsideRuntimeClient.Current.ReceiveResponse(response);
        }

        /// <summary>
        /// Register a timer to send regular callbacks to this grain.
        /// This timer will keep the current grain from being deactivated.
        /// </summary>
        /// <param name="asyncCallback"></param>
        /// <param name="state"></param>
        /// <param name="dueTime"></param>
        /// <param name="period"></param>
        /// <returns></returns>
        public IDisposable RegisterTimer(Func<object, Task> asyncCallback, object state, TimeSpan dueTime, TimeSpan period)
        {
            var ctxt = RuntimeContext.CurrentActivationContext;
            InsideRuntimeClient.Current.Scheduler.CheckSchedulingContextValidity(ctxt);
            String name = ctxt.Name + "Timer";
          
            var timer = GrainTimer.FromTaskCallback(asyncCallback, state, dueTime, period, name);
            timer.Start();
            return timer;
        }

        public override string ToString()
        {
            return String.Format("[{0}SystemTarget: {1}{2}{3}]",
                 SchedulingContext.IsSystemPriorityContext ? String.Empty : "LowPriority",
                 Silo,
                 GrainId,
                 ActivationId);
        }

        public string ToDetailedString()
        {
            return String.Format("{0} CurrentlyExecuting={1}", ToString(), running != null ? running.ToString() : "null");
        }
    }
}
