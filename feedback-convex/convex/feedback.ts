import { mutation } from "./_generated/server";
import { v } from "convex/values";

export const submit = mutation({
  args: { issue: v.string(), timestamp: v.number() },
  handler: async (ctx, args) => {
    await ctx.db.insert("feedback", { issue: args.issue, timestamp: args.timestamp });
  },
});
