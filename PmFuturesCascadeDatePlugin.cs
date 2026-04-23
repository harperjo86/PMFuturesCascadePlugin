using System;
using System.Linq;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;

namespace PMFuturesCascadePlugin
{
    /// <summary>
    /// Fires on Update of dct_pmfutures (Post-Operation, Synchronous).
    ///
    /// Business rule: when dct_scheduledate changes, shift all sibling records
    /// (same dct_mptitle + dct_siteid) by the same delta. context.Depth guards
    /// against re-entrancy — sibling updates fire this plugin at Depth 2, which
    /// exits immediately, preventing any cascade chain beyond depth 1.
    ///
    /// Registration checklist
    /// ─────────────────────
    /// Message   : Update
    /// Entity    : dct_pmfutures
    /// Stage     : 40 – Post-Operation
    /// Mode      : Synchronous
    /// Pre-Image : alias = "PreImage"
    ///             Attributes: dct_scheduledate, dct_mptitle, dct_siteid
    /// Filtering : dct_scheduledate
    /// </summary>
    public class PmFuturesCascadeDatePlugin : IPlugin
    {
        public void Execute(IServiceProvider serviceProvider)
        {
            var context        = (IPluginExecutionContext)    serviceProvider.GetService(typeof(IPluginExecutionContext));
            var serviceFactory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            var tracer         = (ITracingService)             serviceProvider.GetService(typeof(ITracingService));

            // Sibling updates triggered by this plugin arrive at Depth 2 — exit immediately.
            if (context.Depth > 1)
            {
                tracer.Trace($"Depth {context.Depth} — skipping to prevent cascade chain.");
                return;
            }

            if (!context.InputParameters.TryGetValue("Target", out var raw) || !(raw is Entity target))
                return;

            if (!target.Contains("dct_scheduledate"))
                return;

            if (!context.PreEntityImages.TryGetValue("PreImage", out var pre))
            {
                tracer.Trace("PreImage 'PreImage' not registered — plugin cannot run.");
                return;
            }

            // ── Compute delta ────────────────────────────────────────────────────────
            var newDate = target.GetAttributeValue<DateTime?>("dct_scheduledate");
            var oldDate = pre.GetAttributeValue<DateTime?>("dct_scheduledate");

            if (newDate == null || oldDate == null || newDate.Value.Date == oldDate.Value.Date)
                return;

            int deltaDays = (newDate.Value.Date - oldDate.Value.Date).Days;

            // ── Resolve key fields from target → fallback to pre-image ───────────────
            var mpTitle = GetString(target, pre, "dct_mptitle");
            if (string.IsNullOrWhiteSpace(mpTitle))
            {
                tracer.Trace("dct_mptitle is blank — nothing to cascade.");
                return;
            }

            var siteId = GetString(target, pre, "dct_siteid");

            tracer.Trace($"Cascading {deltaDays:+#;-#;0} day(s) | mptitle='{mpTitle}' | siteid={siteId ?? "(any)"}");

            // ── Query siblings ───────────────────────────────────────────────────────
            var query = new QueryExpression("dct_pmfutures")
            {
                ColumnSet = new ColumnSet("dct_pmfuturesid", "dct_scheduledate"),
                NoLock    = true,
                PageInfo  = new PagingInfo { Count = 5000, PageNumber = 1 }
            };
            query.Criteria.AddCondition("dct_mptitle",      ConditionOperator.Equal,    mpTitle);
            query.Criteria.AddCondition("dct_pmfuturesid",  ConditionOperator.NotEqual, target.Id);
            query.Criteria.AddCondition("dct_scheduledate", ConditionOperator.NotNull);

            if (!string.IsNullOrWhiteSpace(siteId))
                query.Criteria.AddCondition("dct_siteid", ConditionOperator.Equal, siteId);

            var service  = serviceFactory.CreateOrganizationService(context.UserId);
            var siblings = service.RetrieveMultiple(query);

            if (siblings.Entities.Count == 0)
            {
                tracer.Trace("No sibling records found — nothing to update.");
                return;
            }

            tracer.Trace($"Updating {siblings.Entities.Count} sibling record(s).");

            // Update in safe order to avoid transient duplicate key violations within
            // the same mptitle+siteid group. Shifting forward: move the latest dates
            // first so each record steps into a slot just vacated. Shifting backward:
            // move the earliest dates first for the same reason.
            var ordered = deltaDays > 0
                ? siblings.Entities.OrderByDescending(e => e.GetAttributeValue<DateTime?>("dct_scheduledate"))
                : siblings.Entities.OrderBy(e => e.GetAttributeValue<DateTime?>("dct_scheduledate"));

            int total = 0;
            foreach (var sibling in ordered)
            {
                var siblingDate = sibling.GetAttributeValue<DateTime?>("dct_scheduledate");
                if (siblingDate == null) continue;

                service.Update(new Entity("dct_pmfutures", sibling.Id)
                {
                    ["dct_scheduledate"] = siblingDate.Value.AddDays(deltaDays)
                });
                total++;
            }

            tracer.Trace($"Cascade complete — {total} record(s) updated.");
        }

        private static string GetString(Entity target, Entity pre, string attribute) =>
            target.Contains(attribute)
                ? target.GetAttributeValue<string>(attribute)
                : pre.GetAttributeValue<string>(attribute);
    }
}
