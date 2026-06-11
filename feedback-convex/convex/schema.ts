import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";

export default defineSchema({
  feedback: defineTable({
    issue: v.string(),
    timestamp: v.number(),
  }),
});
