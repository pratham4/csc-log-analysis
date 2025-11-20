import React from "react";
import {
  Box,
  Typography,
  Card,
  CardContent,
  Chip,
  Button,
} from "@mui/material";

interface StructuredContentProps {
  content: any;
  onSuggestionClick?: (suggestion: string) => void;
  onDirectConfirmation?: (operation: string, confirmed: boolean, operationData: any) => Promise<void>;
}

const StructuredContentRenderer: React.FC<StructuredContentProps> = ({
  content,
  onSuggestionClick,
  onDirectConfirmation,
}) => {
  if (!content || typeof content !== "object") {
    return null;
  }

  const renderStatsCard = (data: any) => (
    <Card
      elevation={0}
      sx={{
        maxWidth: "390px",
        backgroundColor: "#F0F0F0",
        borderRadius: "16px",
      }}
    >
      <CardContent sx={{ p: 2 }}>
        <Box sx={{ display: "flex", alignItems: "center", gap: 2, mb: 1.5 }}>
          <Box>
            <img
              src="/cloud_bot_colored.svg"
              alt="AI"
              style={{ width: 40, height: 40 }}
            />
          </Box>
          <Box sx={{ display: "flex", alignItems: "center" }}>
            <Box>
              <Typography
                variant="subtitle1"
                sx={{ fontWeight: 700, color: "#0f172a", fontSize: "1rem" }}
              >
                {data.title}
              </Typography>
              <Typography
                variant="body2"
                sx={{ color: "#000000", fontSize: "0.8rem" }}
              >
                Region: <strong>{data.region}</strong> {data.table_name && <>• Table: <strong>{data.table_name}</strong></>} {data.filter_description && <>• <strong>{data.filter_description}</strong></>}
              </Typography>
            </Box>
          </Box>
        </Box>

        <Box sx={{ display: "flex", flexWrap: "wrap", gap: 1.5 }}>
          {data.stats?.map((stat: any, index: number) => (
            <Box key={index} sx={{ flex: "1 1 auto" }}>
              <Box
                sx={{
                  p: 1.2,
                  borderRadius: "6px",
                  backgroundColor: "#ffffff",
                  textAlign: "center",
                }}
              >
                <Box
                  sx={{
                    display: "flex",
                    justifyContent: "center",
                    alignItems: "center",
                    gap: 0.5,
                  }}
                >
                  <Typography
                    variant="h6"
                    sx={{
                      fontWeight: 700,
                      color: stat.highlight ? "#0ea5e9" : "#0f172a",
                      fontSize: stat.type === "number" ? "1.2rem" : "0.9rem",
                      lineHeight: 1.2,
                    }}
                  >
                    {stat.value}
                  </Typography>
                </Box>
                <Typography
                  variant="body2"
                  sx={{
                    color: "#000000",
                    mb: 0.25,
                    fontWeight: 500,
                    fontSize: "0.75rem",
                    textAlign: "center",
                  }}
                >
                  {stat.label == "None"
                    ? "Total records"
                    : stat.label.charAt(0).toUpperCase() + stat.label.slice(1)}
                </Typography>
              </Box>
            </Box>
          ))}
        </Box>
      </CardContent>
    </Card>
  );

  const renderConfirmationCard = (data: any) => {
    const isDelete = data.title?.toLowerCase().includes("delete");

    return (
      <Card
        elevation={0}
        sx={{
          backgroundColor: "#F0F0F0",
          borderRadius: "16px",
          maxWidth: "400px",
        }}
      >
        <CardContent sx={{ p: 2, textAlign: "center" }}>
          <Box sx={{ mb: 2, textAlign: "left" }}>
            <Box
              sx={{ display: "flex", alignItems: "center", gap: 2, mb: 1.5 }}
            >
              <Box>
                <img
                  src="/cloud_bot_colored.svg"
                  alt="AI"
                  style={{ width: 40, height: 40 }}
                />
              </Box>
              <Box
                sx={{
                  display: "flex",
                  alignItems: "left",
                  flexDirection: "column",
                  justifyContent: "center",
                }}
              >
                <Box>
                  <Typography
                    variant="subtitle1"
                    sx={{
                      fontWeight: 700,
                      color: "#0f172a",
                      fontSize: "1rem",
                      mb: 0.5,
                    }}
                  >
                    {data.title}
                  </Typography>
                </Box>
                {data.table && (
                  <Box>
                    <Typography
                      variant="body2"
                      sx={{
                        color: "#535b67ff",
                        fontWeight: 500,
                        fontSize: "0.85rem",
                        mt: 0.25,
                      }}
                    >
                      Table : <strong>{data.table}</strong>
                    </Typography>
                  </Box>
                )}
              </Box>
            </Box>
          </Box>

          <Box
            sx={{
              border: "2px solid #f87171",
              borderRadius: "8px",
              py: 2,
              px: 6,
              mb: 2,
              backgroundColor: "#ffffff",
              width: "100%",
              maxWidth: "320px",
              mx: "auto",
            }}
          >
            <Typography
              variant="h2"
              sx={{
                fontWeight: 700,
                color: "#dc2626",
                fontSize: "2.5rem",
                lineHeight: 1,
                mb: 0.5,
              }}
            >
              {data.count?.toLocaleString() || "0"}
            </Typography>
            <Typography
              variant="body1"
              sx={{ color: "#374151", fontWeight: 500, fontSize: "1rem" }}
            >
              Records to {isDelete ? "delete" : "archive"}
            </Typography>
          </Box>

          {/* Instructions */}
          <Typography
            variant="body2"
            sx={{ color: "#747880ff", mb: 3, fontSize: "0.75rem" }}
          >
            {data.instructions ||
              `Click "CONFIRM ${
                isDelete ? "DELETE" : "ARCHIVE"
              }" to proceed or "CANCEL" to abort.`}
          </Typography>

          {/* Delete warning */}
          {isDelete && (
            <Typography
              sx={{
                fontWeight: 600,
                fontSize: "0.85rem",
                color: "#b91c1c",
                mb: 2,
              }}
            >
              This action cannot be undone!
            </Typography>
          )}

          {/* Action buttons */}
          <Box sx={{ display: "flex", justifyContent: "center", gap: 2 }}>
            <Button
              variant="contained"
              color="error"
              onClick={() => {
                if (onDirectConfirmation) {
                  onDirectConfirmation(isDelete ? "DELETE" : "ARCHIVE", true, data);
                } else {
                  onSuggestionClick?.(
                    `CONFIRM ${isDelete ? "DELETE" : "ARCHIVE"}`
                  );
                }
              }}
              sx={{
                backgroundColor: "#dc2626",
                color: "#ffffff",
                fontWeight: 500,
                fontSize: "0.9rem",
                px: 3,
                py: 1,
                borderRadius: "6px",
                textTransform: "none",
                minWidth: "160px",
                "&:hover": {
                  backgroundColor: "#b91c1c",
                  transform: "translateY(-1px)",
                },
                transition: "all 0.2s ease-in-out",
              }}
            >
              CONFIRM {isDelete ? "DELETE" : "ARCHIVE"}
            </Button>
            <Button
              variant="outlined"
              onClick={() => {
                if (onDirectConfirmation) {
                  onDirectConfirmation(isDelete ? "DELETE" : "ARCHIVE", false, data);
                } else {
                  onSuggestionClick?.("CANCEL");
                }
              }}
              sx={{
                backgroundColor: "#ffffff",
                color: "#6b7280",
                fontWeight: 500,
                fontSize: "0.9rem",
                px: 3,
                py: 1,
                borderRadius: "6px",
                textTransform: "none",
                minWidth: "100px",
                border: "1px solid #d1d5db",
                "&:hover": {
                  backgroundColor: "#f9fafb",
                  borderColor: "#9ca3af",
                  transform: "translateY(-1px)",
                },
                transition: "all 0.2s ease-in-out",
              }}
            >
              CANCEL
            </Button>
          </Box>
        </CardContent>
      </Card>
    );
  };

  const renderSuccessCard = (data: any) => (
    <Card
      elevation={0}
      sx={{
        borderRadius: "16px",
        maxWidth: "410px",
        backgroundColor: "#F0F0F0",
      }}
    >
      <CardContent sx={{ p: 2 }}>
        <Box sx={{ display: "flex", alignItems: "center", gap: 2, mb: 1.5 }}>
          <Box>
            <img
              src="/cloud_bot_colored.svg"
              alt="AI"
              style={{ width: 40, height: 40 }}
            />
          </Box>
          <Box>
            <Typography
              variant="subtitle1"
              sx={{ fontWeight: 700, color: "#0f172a", fontSize: "1rem" }}
            >
              {data.title}
            </Typography>
            <Typography
              variant="body2"
              sx={{ color: "#000000", fontSize: "0.8rem" }}
            >
              Region: <strong>{data.region}</strong>
            </Typography>
          </Box>
        </Box>

        {data.details && (
          <Box sx={{ mt: 1.5 }}>
            {data.details.map((detail: any, index: number) => (
              <Typography
                key={index}
                variant="body2"
                sx={{ color: "#374151", mb: 0.25, fontSize: "0.8rem" }}
              >
                • {detail}
              </Typography>
            ))}
          </Box>
        )}
      </CardContent>
    </Card>
  );

  const renderConversationalCard = (data: any) => (
    <Card
      elevation={0}
      sx={{
        borderRadius: "16px",
        maxWidth: "100%",
        width: "100%",
        backgroundColor: "#F0F0F0",
      }}
    >
      <CardContent sx={{ p: 2 }}>
        <Box sx={{ display: "flex", alignItems: "center", gap: 2, mb: 1.5 }}>
          <Box>
            <img
              src="/cloud_bot_colored.svg"
              alt="AI"
              style={{ width: 40, height: 40 }}
            />
          </Box>
          <Box>
            <Typography
              variant="subtitle1"
              sx={{ fontWeight: 700, color: "#0f172a", fontSize: "1rem" }}
            >
              {data.title}
            </Typography>
            <Typography
              variant="body2"
              sx={{ color: "#64748b", fontSize: "0.8rem" }}
            >
              {data.region} • {data.user_role} Access
            </Typography>
          </Box>
        </Box>

        <Box sx={{ mb: 1.5 }}>
          <Typography
            variant="body2"
            sx={{
              color: "#374151",
              fontSize: "0.85rem",
              lineHeight: 1.5,
              whiteSpace: "pre-wrap",
            }}
          >
            {data.content}
          </Typography>
        </Box>
      </CardContent>
    </Card>
  );

  const renderErrorCard = (data: any) => (
    <Card
      elevation={0}
      sx={{
        maxWidth: "100%",
        width: "100%",
        backgroundColor: "#F0F0F0",
        borderRadius: "16px",
      }}
    >
      <CardContent sx={{ p: 2 }}>
        <Box sx={{ display: "flex", alignItems: "center", gap: 2, mb: 1.5 }}>
          <Box>
            <img
              src="/cloud_bot_colored.svg"
              alt="AI"
              style={{ width: 40, height: 40 }}
            />
          </Box>
          <Box>
            <Typography
              variant="subtitle1"
              sx={{ fontWeight: 700, color: "#0f172a", fontSize: "1rem" }}
            >
              {data.title}
            </Typography>
            <Typography
              variant="body2"
              sx={{ color: "#64748b", fontSize: "0.8rem" }}
            >
              {data.region} Region
            </Typography>
          </Box>
        </Box>

        <Typography sx={{ fontWeight: 600, fontSize: "1rem" }}>
          {data.error_message &&
            "I am unable to assist on this request. Please try again with different phrasing."}
        </Typography>

        {data.suggestions && data.suggestions.length > 0 && (
          <Box sx={{ mt: 1.5 }}>
            <Typography
              variant="caption"
              sx={{
                opacity: 0.8,
                mb: 1,
                display: "block",
                fontWeight: 600,
                textTransform: "uppercase",
                fontSize: "0.7rem",
                letterSpacing: "0.05em",
              }}
            >
              Suggested Actions
            </Typography>
            <Box sx={{ display: "flex", flexWrap: "wrap", gap: 1 }}>
              {data.suggestions.map((suggestion: string, index: number) => (
                <Chip
                  key={index}
                  label={suggestion}
                  size="small"
                  clickable
                  onClick={() => onSuggestionClick?.(suggestion)}
                  sx={{
                    fontSize: "0.75rem",
                    height: "28px",
                    borderRadius: "14px",
                    background: "rgba(255, 255, 255, 0.9)",
                    border: "1px solid rgba(0, 169, 206, 0.3)",
                    color: "text.primary",
                    backdropFilter: "blur(10px)",
                    transition: "all 0.2s ease-in-out",
                    "&:hover": {
                      background: "rgba(0, 169, 206, 0.1)",
                      transform: "translateY(-1px)",
                      boxShadow: "0 4px 12px rgba(0, 0, 0, 0.15)",
                    },
                  }}
                />
              ))}
            </Box>
          </Box>
        )}
      </CardContent>
    </Card>
  );

  const renderCancelledCard = (data: any) => (
    <Card
      elevation={0}
      sx={{
        maxWidth: "380px",
        backgroundColor: "#F0F0F0",
        borderRadius: "16px",
      }}
    >
      <CardContent sx={{ p: 2 }}>
        <Box sx={{ display: "flex", alignItems: "center", gap: 2, mb: 1.5 }}>
          <Box>
            <img
              src="/cloud_bot_colored.svg"
              alt="AI"
              style={{ width: 40, height: 40 }}
            />
          </Box>
          <Box>
            <Typography
              variant="subtitle1"
              sx={{ fontWeight: 700, color: "#0f172a", fontSize: "1rem" }}
            >
              {data.title}
            </Typography>
            <Typography
              variant="body2"
              sx={{ color: "#000000", fontSize: "0.8rem" }}
            >
              Region: <strong>{data.region}</strong> • Table: <strong>{data.table}</strong>
            </Typography>
          </Box>
        </Box>

        <Box sx={{ mb: 1.5 }}>
          <Typography
            variant="body2"
            sx={{
              color: "#000000",
              fontSize: "0.85rem",
              lineHeight: 1.5,
              mb: 1,
            }}
          >
            {data.message}
          </Typography>
          {data.details &&
            data.details.map((detail: string, index: number) => (
              <Typography
                key={index}
                variant="body2"
                sx={{ color: "#6b7280", fontSize: "0.8rem", mb: 0.25 }}
              >
                • {detail}
              </Typography>
            ))}
        </Box>
      </CardContent>
    </Card>
  );

  const renderClarificationCard = (data: any) => (
    <Card
      elevation={0}
      sx={{
        borderRadius: "16px",
        maxWidth: "100%",
        width: "100%",
        backgroundColor: "#F0F0F0",
      }}
    >
      <CardContent sx={{ p: 2 }}>
        <Box sx={{ display: "flex", alignItems: "center", gap: 2, mb: 1.5 }}>
          <Box>
            <img
              src="/cloud_bot_colored.svg"
              alt="AI"
              style={{ width: 40, height: 40 }}
            />
          </Box>
          <Box>
            <Typography
              variant="subtitle1"
              sx={{ fontWeight: 700, color: "#0f172a", fontSize: "1rem" }}
            >
              {data.title}
            </Typography>
            <Typography
              variant="body2"
              sx={{ color: "#000000", fontSize: "0.8rem" }}
            >
              Region: <strong>{data.region}</strong>
            </Typography>
          </Box>
        </Box>

        <Box sx={{ mb: 1.5 }}>
          <Typography
            variant="body2"
            sx={{
              color: "#374151",
              fontSize: "0.85rem",
              lineHeight: 1.5,
              whiteSpace: "pre-wrap",
            }}
          >
            {data.message}
          </Typography>
        </Box>

        {data.suggestions && data.suggestions.length > 0 && (
          <Box sx={{ mt: 1.5 }}>
            <Typography
              variant="caption"
              sx={{
                opacity: 0.8,
                mb: 1,
                display: "block",
                fontWeight: 600,
                textTransform: "uppercase",
                fontSize: "0.7rem",
                letterSpacing: "0.05em",
              }}
            >
              Suggestions
            </Typography>
            <Box sx={{ display: "flex", flexWrap: "wrap", gap: 1 }}>
              {data.suggestions.map((suggestion: string, index: number) => (
                <Chip
                  key={index}
                  label={suggestion}
                  size="small"
                  clickable
                  onClick={() => onSuggestionClick?.(suggestion)}
                  sx={{
                    fontSize: "0.75rem",
                    height: "28px",
                    borderRadius: "14px",
                    background: "rgba(255, 255, 255, 0.9)",
                    border: "1px solid rgba(0, 169, 206, 0.3)",
                    color: "text.primary",
                    backdropFilter: "blur(10px)",
                    transition: "all 0.2s ease-in-out",
                    "&:hover": {
                      background: "rgba(190, 233, 243, 0.1)",
                      transform: "translateY(-1px)",
                      boxShadow: "0 4px 12px rgba(0, 0, 0, 0.1)",
                    },
                  }}
                />
              ))}
            </Box>
          </Box>
        )}
      </CardContent>
    </Card>
  );

  const renderWelcomeCard = (data: any) => (
    <Card
      elevation={0}
      sx={{
        borderRadius: "16px",
        maxWidth: "100%",
        width: "100%",
        backgroundColor: "#F0F0F0",
      }}
    >
      <CardContent sx={{ p: 2 }}>
        <Box sx={{ display: "flex", alignItems: "center", gap: 2, mb: 1.5 }}>
          <Box>
            <img
              src="/cloud_bot_colored.svg"
              alt="AI"
              style={{ width: 40, height: 40 }}
            />
          </Box>
          <Box>
            <Typography
              variant="subtitle1"
              sx={{ fontWeight: 700, color: "#0f172a", fontSize: "1rem" }}
            >
              {data.title}
            </Typography>
            <Typography
              variant="body2"
              sx={{ color: "#64748b", fontSize: "0.8rem" }}
            >
              {data.region} • {data.user_role} Access
            </Typography>
          </Box>
        </Box>

        <Box sx={{ mb: 1.5 }}>
          <Typography
            variant="body2"
            sx={{
              color: "#374151",
              fontSize: "1rem",
              lineHeight: 1.5,
              whiteSpace: "pre-wrap",
              fontWeight: 600,
            }}
          >
            {data.content}
          </Typography>
        </Box>
      </CardContent>
    </Card>
  );

  const renderJobLogsTableCard = (data: any) => (
    <Card
      elevation={0}
      sx={{
        width: "100%",
        maxWidth: "100%",
        backgroundColor: "#F0F0F0",
        borderRadius: "16px",
      }}
    >
      <CardContent sx={{ p: 2 }}>
        <Box sx={{ display: "flex", alignItems: "center", gap: 2, mb: 2 }}>
          <Box>
            <img
              src="/cloud_bot_colored.svg"
              alt="AI"
              style={{ width: 40, height: 40 }}
            />
          </Box>
          <Box>
            <Typography
              variant="subtitle1"
              sx={{ fontWeight: 700, color: "#000000", fontSize: "1rem" }}
            >
              {data.title || "Job Logs"}
            </Typography>
            <Typography
              variant="body2"
              sx={{ color: "#00000", fontSize: "0.8rem" }}
            >
              Region <strong>{data.region} </strong>
              {(data.records?.length) > 1 && (
                <> • Showing {data.records?.length || 0} of {data.total_count || 0} jobs</>
              )}
            </Typography>
          </Box>
        </Box>



        {/* Job logs table */}
        {data.records && Array.isArray(data.records) && data.records.length > 0 && (
          <Box sx={{ overflowX: "auto" }}>
            <Box sx={{ minWidth: "800px" }}>
              {/* Table header */}
              <Box sx={{ 
                display: "grid", 
                gridTemplateColumns: "90px 160px 90px 90px 130px 130px 300px",
                gap: 1,
                p: 1.5,
                backgroundColor: "#e5e7eb",
                borderRadius: "8px 8px 0 0"
              }}>
                <Typography variant="caption" sx={{ fontSize: "0.75rem", fontWeight: 600, color: "#374151" }}>Type</Typography>
                <Typography variant="caption" sx={{ fontSize: "0.75rem", fontWeight: 600, color: "#374151" }}>Table</Typography>
                <Typography variant="caption" sx={{ fontSize: "0.75rem", fontWeight: 600, color: "#374151" }}>Status</Typography>
                <Typography variant="caption" sx={{ fontSize: "0.75rem", fontWeight: 600, color: "#374151" }}>Records</Typography>
                <Typography variant="caption" sx={{ fontSize: "0.75rem", fontWeight: 600, color: "#374151" }}>Started</Typography>
                <Typography variant="caption" sx={{ fontSize: "0.75rem", fontWeight: 600, color: "#374151" }}>Finished</Typography>
                <Typography variant="caption" sx={{ fontSize: "0.75rem", fontWeight: 600, color: "#374151" }}>Reason</Typography>
              </Box>

              {/* Table rows */}
              {data.records.map((record: any, index: number) => {
                
                return (
                  <Box key={record.id || index} sx={{
                    display: "grid",
                    gridTemplateColumns: "90px 160px 90px 90px 130px 130px 300px",
                    gap: 1,
                    p: 1.5,
                    backgroundColor: index % 2 === 0 ? "#ffffff" : "#f9fafb",
                    borderBottom: index === data.records.length - 1 ? "none" : "1px solid #e5e7eb",
                    alignItems: "center"
                  }}>
                    <Box>
                      <Chip 
                        label={record?.job_type || 'UNKNOWN'} 
                        size="small" 
                        sx={{ 
                          backgroundColor: (() => {
                            switch(record?.job_type) {
                              case 'ARCHIVE': return '#3b82f6';
                              case 'DELETE': return '#ef4444';
                              case 'OTHER': return '#6b7280';
                              default: return '#6b7280';
                            }
                          })(),
                          color: "white", 
                          fontSize: "0.7rem",
                          height: "20px"
                        }} 
                      />
                    </Box>
                    <Typography variant="body2" sx={{ fontWeight: 500, color: "#111827", fontSize: "0.8rem" }}>
                      {record?.table_name || '-'}
                    </Typography>
                    <Box>
                      <Chip 
                        label={record?.status || 'UNKNOWN'} 
                        size="small" 
                        sx={{ 
                          backgroundColor: (() => {
                            switch(record?.status) {
                              case 'SUCCESS': return '#10b981';
                              case 'FAILED': return '#ef4444';
                              case 'IN_PROGRESS': return '#f59e0b';
                              default: return '#6b7280';
                            }
                          })(),
                          color: "white", 
                          fontSize: "0.7rem",
                          height: "20px"
                        }} 
                      />
                    </Box>
                    <Typography variant="body2" sx={{ textAlign: "left", fontWeight: 600, color: "#111827", fontSize: "0.8rem" }}>
                      {record?.records_affected?.toLocaleString() || "0"}
                    </Typography>
                    <Typography variant="body2" sx={{ fontFamily: "monospace", color: "#6b7280", fontSize: "0.8rem" }}>
                      {(() => {
                        if (!record?.started_at) return '-';
                        try {
                          return new Date(record.started_at).toLocaleString('en-US', {
                            month: '2-digit',
                            day: '2-digit',
                            hour: '2-digit',
                            minute: '2-digit'
                          });
                        } catch (e) {
                          return String(record.started_at).substring(0, 16);
                        }
                      })()}
                    </Typography>
                    <Typography variant="body2" sx={{ fontFamily: "monospace", color: "#6b7280", fontSize: "0.8rem" }}>
                      {(() => {
                        if (!record?.finished_at) return '-';
                        try {
                          return new Date(record.finished_at).toLocaleString('en-US', {
                            month: '2-digit',
                            day: '2-digit',
                            hour: '2-digit',
                            minute: '2-digit'
                          });
                        } catch (e) {
                          return String(record.finished_at).substring(0, 16);
                        }
                      })()}
                    </Typography>
                    <Typography 
                      variant="body2" 
                      sx={{ 
                        color: "#6b7280", 
                        fontSize: "0.8rem",
                        lineHeight: 1.4,
                        wordBreak: "break-word"
                      }} 
                      title={record?.reason || ''}
                    >
                      {record?.reason || '-'}
                    </Typography>
                  </Box>
                );
              })}
            </Box>
          </Box>
        )}

        {/* Pagination info */}
        {(data.total_count || 0) > (data.records?.length || 0) && (data.records?.length || 0) > 0 && (
          <Box sx={{ mt: 2, textAlign: "center" }}>
            <Typography variant="body2" sx={{ color: "#6b7280", fontSize: "0.8rem" }}>
              {data.records?.length > 1 && `Showing ${data.records?.length } of ${data.total_count} total records`}
            </Typography>
          </Box>
        )}

        {/* Empty state for no records */}
        {(!data.records || data.records.length === 0) && (
          <Box sx={{ textAlign: "center", py: 4 }}>
            <Typography variant="body2" sx={{ color: "#6b7280", fontSize: "0.9rem" }}>
              No job logs found matching the current criteria.
            </Typography>
          </Box>
        )}

        {/* Suggestions */}
        {data.suggestions && data.suggestions.length > 0 && (
          <Box sx={{ mt: 2 }}>
            <Typography
              variant="caption"
              sx={{
                opacity: 0.8,
                mb: 1,
                display: "block",
                fontWeight: 600,
                textTransform: "uppercase",
                fontSize: "0.7rem",
                letterSpacing: "0.05em",
              }}
            >
              Suggestions
            </Typography>
            <Box sx={{ display: "flex", flexWrap: "wrap", gap: 1 }}>
              {data.suggestions.map((suggestion: string, index: number) => (
                <Chip
                  key={index}
                  label={suggestion}
                  size="small"
                  clickable
                  onClick={() => onSuggestionClick?.(suggestion)}
                  sx={{
                    fontSize: "0.75rem",
                    height: "28px",
                    borderRadius: "14px",
                    background: "rgba(255, 255, 255, 0.9)",
                    border: "1px solid rgba(0, 169, 206, 0.3)",
                    color: "text.primary",
                    backdropFilter: "blur(10px)",
                    transition: "all 0.2s ease-in-out",
                    "&:hover": {
                      background: "rgba(0, 169, 206, 0.1)",
                      transform: "translateY(-1px)",
                      boxShadow: "0 4px 12px rgba(0, 0, 0, 0.15)",
                    },
                  }}
                />
              ))}
            </Box>
          </Box>
        )}
      </CardContent>
    </Card>
  );

  const renderDatabaseOverviewCard = (data: any) => (
    <Card
      elevation={0}
      sx={{
        width: "100%",
        maxWidth: "100%",
        backgroundColor: "#F0F0F0",
        borderRadius: "16px",
      }}
    >
      <CardContent sx={{ p: 2 }}>
        <Box sx={{ display: "flex", alignItems: "center", gap: 2, mb: 2 }}>
          <Box>
            <img
              src="/cloud_bot_colored.svg"
              alt="AI"
              style={{ width: 40, height: 40 }}
            />
          </Box>
          <Box>
            <Typography
              variant="subtitle1"
              sx={{ fontWeight: 700, color: "#000000", fontSize: "1rem" }}
            >
              Database Statistics
            </Typography>
            <Typography
              variant="body2"
              sx={{ color: "#000000", fontSize: "0.8rem" }}
            >
              Region: <strong>{data.region}</strong> • {data.summary?.main_tables_count || 0} main
              tables • {data.summary?.archive_tables_count || 0} archive tables
            </Typography>
          </Box>
        </Box>

        {/* Main Tables */}
        {data.main_tables && data.main_tables.length > 0 && (
          <Box sx={{ mb: 2 }}>
            <Typography
              variant="subtitle2"
              sx={{
                fontWeight: 600,
                color: "#000000",
                mb: 1,
                fontSize: "0.9rem",
              }}
            >
              🗂️ Main Tables
            </Typography>
            <Box sx={{ display: "flex", flexDirection: "column", gap: 1 }}>
              {data.main_tables.map((table: any, index: number) => (
                <Box
                  key={index}
                  sx={{
                    p: 1.5,
                    borderRadius: "8px",
                    backgroundColor: "#ffffff",
                    display: "flex",
                    justifyContent: "space-between",
                    alignItems: "center",
                  }}
                >
                  <Box>
                    <Typography
                      variant="subtitle2"
                      sx={{
                        fontWeight: 600,
                        color: "#000000",
                        fontSize: "0.85rem",
                      }}
                    >
                      {table.name}
                    </Typography>
                    {table.error ? (
                      <Typography
                        variant="body2"
                        sx={{ color: "#ef4444", fontSize: "0.75rem" }}
                      >
                        Error: {table.error}
                      </Typography>
                    ) : (
                      <Typography
                        variant="body2"
                        sx={{ color: "#64748b", fontSize: "0.75rem" }}
                      >
                        Records older than {table.age_days} days:{" "}
                        <Typography
                          component="span"
                          sx={{
                            fontWeight: 900,
                            color: "#f59e0b",
                            fontSize: "0.8rem",
                          }}
                        >
                          {table.age_based_count?.toLocaleString() || "0"}
                        </Typography>
                      </Typography>
                    )}
                  </Box>
                  <Box sx={{ textAlign: "right" }}>
                    <Typography
                      variant="subtitle2"
                      sx={{
                        fontWeight: 900,
                        color: "#0ea5e9",
                        fontSize: "1.0rem",
                      }}
                    >
                      {table.total_records?.toLocaleString() || "0"}
                    </Typography>
                    <Typography
                      variant="caption"
                      sx={{ color: "#64748b", fontSize: "0.65rem" }}
                    >
                      Total records
                    </Typography>
                  </Box>
                </Box>
              ))}
            </Box>
          </Box>
        )}

        {/* Archive Tables */}
        {data.archive_tables && data.archive_tables.length > 0 && (
          <Box>
            <Typography
              variant="subtitle2"
              sx={{
                fontWeight: 600,
                color: "#000000",
                mb: 1,
                fontSize: "0.9rem",
              }}
            >
              📦 Archive Tables
            </Typography>
            <Box sx={{ display: "flex", flexDirection: "column", gap: 1 }}>
              {data.archive_tables.map((table: any, index: number) => (
                <Box
                  key={index}
                  sx={{
                    p: 1.5,
                    borderRadius: "8px",
                    backgroundColor: "#ffffff",
                    display: "flex",
                    justifyContent: "space-between",
                    alignItems: "center",
                  }}
                >
                  <Box>
                    <Typography
                      variant="subtitle2"
                      sx={{
                        fontWeight: 600,
                        color: "#000000",
                        fontSize: "0.85rem",
                      }}
                    >
                      {table.name}
                    </Typography>
                    {table.error ? (
                      <Typography
                        variant="body2"
                        sx={{ color: "#ef4444", fontSize: "0.75rem" }}
                      >
                        Error: {table.error}
                      </Typography>
                    ) : (
                      <Typography
                        variant="body2"
                        sx={{ color: "#64748b", fontSize: "0.75rem" }}
                      >
                        Records older than {table.age_days} days:{" "}
                        <Typography
                          component="span"
                          sx={{
                            fontWeight: 900,
                            color: "#f59e0b",
                            fontSize: "0.8rem",
                          }}
                        >
                          {table.age_based_count?.toLocaleString() || "0"}
                        </Typography>
                      </Typography>
                    )}
                  </Box>
                  <Box sx={{ textAlign: "right" }}>
                    <Typography
                      variant="subtitle2"
                      sx={{
                        fontWeight: 900,
                        color: "#d97706",
                        fontSize: "1.0rem",
                      }}
                    >
                      {table.total_records?.toLocaleString() || "0"}
                    </Typography>
                    <Typography
                      variant="caption"
                      sx={{ color: "#64748b", fontSize: "0.65rem" }}
                    >
                      Total records
                    </Typography>
                  </Box>
                </Box>
              ))}
            </Box>
          </Box>
        )}
      </CardContent>
    </Card>
  );

  const renderRegionCard = (data: any) => {
    const content = data.content || '';
    const isShortResponse = content.length < 50;
    
    return (
      <Card
        elevation={0}
        sx={{
          width: "100%",
          backgroundColor: "#F0F0F0",
          borderRadius: "16px",
        }}
      >
        <CardContent sx={{ p: 2 }}>
          <Box sx={{ display: "flex", alignItems: "center", gap: 2, mb: 1.5 }}>
            <Box>
              <img
                src="/cloud_bot_colored.svg"
                alt="AI"
                style={{ width: 40, height: 40 }}
              />
            </Box>
            <Box>
              <Typography
                variant="subtitle1"
                sx={{ fontWeight: 700, color: "#000000", fontSize: "1rem" }}
              >
                {data.title || "Region Information"}
              </Typography>
              <Typography
                variant="body2"
                sx={{ color: "#64748b", fontSize: "0.8rem" }}
              >
                🌐 Region Information
              </Typography>
            </Box>
          </Box>

          <Box sx={{ mb: 1.5 }}>
            <Typography
              variant="body1"
              sx={{
                color: "#000000",
                fontSize: isShortResponse ? "1.1rem" : "0.9rem",
                lineHeight: 1.5,
                fontWeight: 500,
                backgroundColor: "#ffffff",
                p: isShortResponse ? 2 : 1.5,
                borderRadius: "8px",
                border: "1px solid #e5e7eb",
                textAlign: "left",
                whiteSpace: "pre-wrap",
              }}
            >
              {content}
            </Typography>
          </Box>
        </CardContent>
      </Card>
    );
  };

  const renderAnalysisCard = (data: any) => (
    <Card
      elevation={0}
      sx={{
        width: "100%",
        backgroundColor: "#F0F0F0",
        borderRadius: "16px",
      }}
    >
      <CardContent sx={{ p: 2 }}>
        <Box sx={{ display: "flex", alignItems: "center", gap: 2, mb: 1.5 }}>
          <Box>
            <img
              src="/cloud_bot_colored.svg"
              alt="AI"
              style={{ width: 40, height: 40 }}
            />
          </Box>
          <Box>
            <Typography
              variant="subtitle1"
              sx={{ fontWeight: 700, color: "#0f172a", fontSize: "1rem" }}
            >
              {data.title || "Intelligent Analysis"}
            </Typography>
            <Typography
              variant="body2"
              sx={{ color: "#64748b", fontSize: "0.8rem" }}
            >
              Region: <strong>{data.region}</strong> • AI Analysis
            </Typography>
          </Box>
        </Box>

        {/* Main analysis content */}
        <Box sx={{ mb: 2 }}>
          <Typography
            variant="body1"
            sx={{
              color: "#374151",
              fontSize: "0.95rem",
              lineHeight: 1.6,
              whiteSpace: "pre-wrap",
              backgroundColor: "#ffffff",
              p: 2,
              borderRadius: "8px",
              border: "1px solid #e5e7eb",
            }}
          >
            {data.analysis_content}
          </Typography>
        </Box>
      </CardContent>
    </Card>
  );

  const renderDSIErrorAnalysisCard = (data: any) => (
    <Card
      elevation={0}
      sx={{
        maxWidth: "500px",
        backgroundColor: "#F0F0F0",
        borderRadius: "16px",
      }}
    >
      <CardContent sx={{ p: 2 }}>
        <Box sx={{ display: "flex", alignItems: "center", gap: 2, mb: 1.5 }}>
          <Box>
            <img
              src="/cloud_bot_colored.svg"
              alt="AI"
              style={{ width: 40, height: 40 }}
            />
          </Box>
          <Box>
            <Typography
              variant="subtitle1"
              sx={{ fontWeight: 700, color: "#0f172a", fontSize: "1rem" }}
            >
              {data.title} - {data.region.toUpperCase()} Region
            </Typography>
            <Typography
              variant="body2"
              sx={{ color: "#64748b", fontSize: "0.85rem" }}
            >
              DSI Error Analysis • {new Date(data.timestamp).toLocaleString()}
            </Typography>
          </Box>
        </Box>

        <Box sx={{ mb: 2 }}>
          {data.data?.period && (
            <Chip
              label={`Period: ${data.data.period}`}
              size="small"
              sx={{ mr: 1, mb: 1, backgroundColor: "#dbeafe", color: "#1e40af" }}
            />
          )}
          {data.data?.instance_filter && (
            <Chip
              label={`Instance: ${data.data.instance_filter}`}
              size="small"
              sx={{ mr: 1, mb: 1, backgroundColor: "#dcfce7", color: "#166534" }}
            />
          )}
          {data.data?.total_errors_found !== undefined && (
            <Chip
              label={`Errors Found: ${data.data.total_errors_found}`}
              size="small"
              sx={{ mr: 1, mb: 1, backgroundColor: "#fee2e2", color: "#dc2626" }}
            />
          )}
        </Box>

        {data.data?.errors && data.data.errors.length > 0 && (
          <Box>
            <Typography variant="subtitle2" sx={{ mb: 1, fontWeight: 600 }}>
              🚨 Error Details:
            </Typography>
            {data.data.errors.slice(0, 5).map((error: any, index: number) => (
              <Box
                key={index}
                sx={{
                  p: 1.5,
                  mb: 1,
                  backgroundColor: "#ffffff",
                  borderRadius: "8px",
                  border: "1px solid #fee2e2",
                }}
              >
                <Box sx={{ display: "flex", justifyContent: "space-between", mb: 0.5 }}>
                  <Typography variant="body2" sx={{ fontWeight: 600, color: "#dc2626" }}>
                    #{index + 1}
                  </Typography>
                  <Chip
                    label={`Count: ${error.occurrence_count || error.error_count}`}
                    size="small"
                    sx={{ backgroundColor: "#fef3c7", color: "#92400e" }}
                  />
                </Box>
                <Typography variant="body2" sx={{ color: "#374151", mb: 0.5 }}>
                  <strong>Instance:</strong> {error.instance_id}
                </Typography>
                <Typography
                  variant="body2"
                  sx={{
                    color: "#6b7280",
                    fontSize: "0.8rem",
                    fontFamily: "monospace",
                    backgroundColor: "#f9fafb",
                    p: 1,
                    borderRadius: "4px",
                    wordBreak: "break-word",
                  }}
                >
                  {error.error_preview || error.error_message}
                </Typography>
              </Box>
            ))}
            {data.data.errors.length > 5 && (
              <Typography variant="body2" sx={{ color: "#6b7280", textAlign: "center", mt: 1 }}>
                ... and {data.data.errors.length - 5} more errors
              </Typography>
            )}
          </Box>
        )}
      </CardContent>
    </Card>
  );

  const renderDSIUserAnalysisCard = (data: any) => (
    <Card
      elevation={0}
      sx={{
        maxWidth: "450px",
        backgroundColor: "#F0F0F0",
        borderRadius: "16px",
      }}
    >
      <CardContent sx={{ p: 2 }}>
        <Box sx={{ display: "flex", alignItems: "center", gap: 2, mb: 1.5 }}>
          <Box>
            <img
              src="/cloud_bot_colored.svg"
              alt="AI"
              style={{ width: 40, height: 40 }}
            />
          </Box>
          <Box>
            <Typography
              variant="subtitle1"
              sx={{ fontWeight: 700, color: "#0f172a", fontSize: "1rem" }}
            >
              {data.title} - {data.region.toUpperCase()} Region
            </Typography>
            <Typography
              variant="body2"
              sx={{ color: "#64748b", fontSize: "0.85rem" }}
            >
              DSI User Analysis • {new Date(data.timestamp).toLocaleString()}
            </Typography>
          </Box>
        </Box>

        <Box sx={{ mb: 2 }}>
          {data.data?.instance_id && (
            <Chip
              label={`Instance: ${data.data.instance_id}`}
              size="small"
              sx={{ mr: 1, mb: 1, backgroundColor: "#dcfce7", color: "#166534" }}
            />
          )}
          {data.data?.period && (
            <Chip
              label={`Period: ${data.data.period}`}
              size="small"
              sx={{ mr: 1, mb: 1, backgroundColor: "#dbeafe", color: "#1e40af" }}
            />
          )}
        </Box>

        {data.data?.users && data.data.users.length > 0 && (
          <Box>
            <Typography variant="subtitle2" sx={{ mb: 1, fontWeight: 600 }}>
              👤 User Error Rankings:
            </Typography>
            {data.data.users.slice(0, 10).map((user: any, index: number) => (
              <Box
                key={index}
                sx={{
                  display: "flex",
                  justifyContent: "space-between",
                  alignItems: "center",
                  p: 1.5,
                  mb: 1,
                  backgroundColor: "#ffffff",
                  borderRadius: "8px",
                  border: "1px solid #e5e7eb",
                }}
              >
                <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
                  <Typography
                    variant="body2"
                    sx={{
                      fontWeight: 700,
                      color: index === 0 ? "#dc2626" : index === 1 ? "#ea580c" : index === 2 ? "#ca8a04" : "#374151",
                      minWidth: "24px",
                    }}
                  >
                    #{index + 1}
                  </Typography>
                  <Typography variant="body2" sx={{ fontWeight: 600 }}>
                    {user.user_id}
                  </Typography>
                </Box>
                <Chip
                  label={`${user.error_count} errors`}
                  size="small"
                  sx={{
                    backgroundColor: index === 0 ? "#fee2e2" : "#f3f4f6",
                    color: index === 0 ? "#dc2626" : "#374151",
                  }}
                />
              </Box>
            ))}
          </Box>
        )}
      </CardContent>
    </Card>
  );

  const renderDSILogAnalysisCard = (data: any) => (
    <Card
      elevation={0}
      sx={{
        maxWidth: "600px",
        backgroundColor: "#F0F0F0",
        borderRadius: "16px",
      }}
    >
      <CardContent sx={{ p: 2 }}>
        <Box sx={{ display: "flex", alignItems: "center", gap: 2, mb: 1.5 }}>
          <Box>
            <img
              src="/cloud_bot_colored.svg"
              alt="AI"
              style={{ width: 40, height: 40 }}
            />
          </Box>
          <Box>
            <Typography
              variant="subtitle1"
              sx={{ fontWeight: 700, color: "#0f172a", fontSize: "1rem" }}
            >
              {data.title} - {data.region.toUpperCase()} Region
            </Typography>
            <Typography
              variant="body2"
              sx={{ color: "#64748b", fontSize: "0.85rem" }}
            >
              DSI Log Analysis • {new Date(data.timestamp).toLocaleString()}
            </Typography>
          </Box>
        </Box>

        <Box sx={{ mb: 2 }}>
          {data.data?.instance_id && (
            <Chip
              label={`Instance: ${data.data.instance_id}`}
              size="small"
              sx={{ mr: 1, mb: 1, backgroundColor: "#dcfce7", color: "#166534" }}
            />
          )}
          {data.data?.time_window && (
            <Chip
              label={`Window: ${data.data.time_window}`}
              size="small"
              sx={{ mr: 1, mb: 1, backgroundColor: "#dbeafe", color: "#1e40af" }}
            />
          )}
          {data.data?.user_filter && (
            <Chip
              label={`User: ${data.data.user_filter}`}
              size="small"
              sx={{ mr: 1, mb: 1, backgroundColor: "#fef3c7", color: "#92400e" }}
            />
          )}
          {data.data?.total_logs !== undefined && (
            <Chip
              label={`Total Logs: ${data.data.total_logs}`}
              size="small"
              sx={{ mr: 1, mb: 1, backgroundColor: "#f3f4f6", color: "#374151" }}
            />
          )}
        </Box>

        {data.data?.target_datetime && (
          <Box sx={{ mb: 2, p: 1.5, backgroundColor: "#ffffff", borderRadius: "8px" }}>
            <Typography variant="body2" sx={{ color: "#374151" }}>
              <strong>🎯 Target Time:</strong> {data.data.target_datetime}
            </Typography>
          </Box>
        )}

        {data.data?.logs && data.data.logs.length > 0 && (
          <Box>
            <Typography variant="subtitle2" sx={{ mb: 1, fontWeight: 600 }}>
              📋 Log Details:
            </Typography>
            <Box sx={{ maxHeight: "300px", overflowY: "auto" }}>
              {data.data.logs.slice(0, 10).map((log: any, index: number) => (
                <Box
                  key={index}
                  sx={{
                    p: 1.5,
                    mb: 1,
                    backgroundColor: log.has_error ? "#fef2f2" : "#ffffff",
                    borderRadius: "8px",
                    border: log.has_error ? "1px solid #fecaca" : "1px solid #e5e7eb",
                  }}
                >
                  <Box sx={{ display: "flex", justifyContent: "space-between", alignItems: "center", mb: 0.5 }}>
                    <Typography
                      variant="body2"
                      sx={{
                        fontWeight: 600,
                        color: log.has_error ? "#dc2626" : "#10b981",
                      }}
                    >
                      {log.has_error ? "❌" : "✅"} {log.when_received}
                    </Typography>
                    <Typography variant="caption" sx={{ color: "#6b7280" }}>
                      {log.table_source}
                    </Typography>
                  </Box>
                  <Typography variant="body2" sx={{ color: "#374151", mb: 0.5 }}>
                    <strong>User:</strong> {log.user_id} • <strong>Function:</strong> {log.function_call_id}
                  </Typography>
                  {log.error_message && (
                    <Typography
                      variant="body2"
                      sx={{
                        color: "#dc2626",
                        fontSize: "0.8rem",
                        fontFamily: "monospace",
                        backgroundColor: "#fef2f2",
                        p: 1,
                        borderRadius: "4px",
                        wordBreak: "break-word",
                      }}
                    >
                      {log.error_message}
                    </Typography>
                  )}
                </Box>
              ))}
            </Box>
            {data.data.logs.length > 10 && (
              <Typography variant="body2" sx={{ color: "#6b7280", textAlign: "center", mt: 1 }}>
                ... and {data.data.logs.length - 10} more logs
              </Typography>
            )}
          </Box>
        )}
      </CardContent>
    </Card>
  );

  const renderAccessDeniedCard = (data: any) => (
    <Card
      elevation={0}
      sx={{
        maxWidth: "420px",
        backgroundColor: "#F0F0F0",
        borderRadius: "16px",
      }}
    >
      <CardContent sx={{ p: 2 }}>
        <Box sx={{ display: "flex", alignItems: "center", gap: 2, mb: 1.5 }}>
          <Box>
            <img
              src="/cloud_bot_colored.svg"
              alt="AI"
              style={{ width: 40, height: 40 }}
            />
          </Box>
          <Box>
            <Typography
              variant="subtitle1"
              sx={{ fontWeight: 700, color: "#0f172a", fontSize: "1rem" }}
            >
              {data.title}
            </Typography>
            <Typography
              variant="body2"
              sx={{ color: "#000000", fontSize: "0.8rem" }}
            >
              Region: <strong>{data.region}</strong> • Role: <strong>{data.user_role}</strong>
            </Typography>
          </Box>
        </Box>

        <Box sx={{ mb: 1.5 }}>
          <Typography
            variant="body2"
            sx={{
              color: "#374151",
              fontSize: "0.85rem",
              lineHeight: 1.5,
              whiteSpace: "pre-wrap",
            }}
          >
            {data.description}
          </Typography>
        </Box>
      </CardContent>
    </Card>
  );

  // Main rendering logic
  switch (content.type) {
    case "stats_card":
      return renderStatsCard(content);
    case "database_overview":
      return renderDatabaseOverviewCard(content);
    case "region_status_card":
      return renderRegionCard(content);
    case "job_logs_table":
      return renderJobLogsTableCard(content);
    case "confirmation_card":
      return renderConfirmationCard(content);
    case "success_card":
      return renderSuccessCard(content);
    case "conversational_card":
      return renderConversationalCard(content);
    case "clarification_card":
      return renderClarificationCard(content);
    case "welcome_card":
      return renderWelcomeCard(content);
    case "error_card":
      return renderErrorCard(content);
    case "cancelled_card":
      return renderCancelledCard(content);
    case "analysis_card":
      return renderAnalysisCard(content);
    case "access_denied_card":
      return renderAccessDeniedCard(content);
    case "dsi_error_analysis":
      return renderDSIErrorAnalysisCard(content);
    case "dsi_user_analysis":
      return renderDSIUserAnalysisCard(content);
    case "dsi_log_analysis":
      return renderDSILogAnalysisCard(content);
    default:
      // Fallback to plain JSON display for unknown types
      return (
        <Box
          sx={{
            p: 2,
            borderRadius: "16px",
            backgroundColor: "#F0F0F0",
            border: "1px solid #e2e8f0",
          }}
        >
          <Typography
            variant="body2"
            component="pre"
            sx={{ whiteSpace: "pre-wrap" }}
          >
            {JSON.stringify(content, null, 2)}
          </Typography>
        </Box>
      );
  }
};

export default StructuredContentRenderer;
