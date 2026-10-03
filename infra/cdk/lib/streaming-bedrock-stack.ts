import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as iam from "aws-cdk-lib/aws-iam";
import * as cloudfront from "aws-cdk-lib/aws-cloudfront";
import * as origins from "aws-cdk-lib/aws-cloudfront-origins";
import * as path from "node:path";

export class StreamingBedrockStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // 1. Define Lambda Function with Node.js 20.x
    const streamingFn = new lambda.Function(this, "BedrockStreamingFunction", {
      runtime: lambda.Runtime.NODEJS_20_X,
      handler: "index.handler",
      code: lambda.Code.fromAsset(path.join(__dirname, "../../../lambda")),
      timeout: cdk.Duration.minutes(5),
      memorySize: 512,
      architecture: lambda.Architecture.ARM_64,
      environment: {
        BEDROCK_MODEL_ID: "us.anthropic.claude-3-7-sonnet-20250219-v1:0",
      },
    });

    // 2. Scoped permissions to Amazon Bedrock ConverseStream & models
    streamingFn.addToRolePolicy(
      new iam.PolicyStatement({
        actions: [
          "bedrock:InvokeModelWithResponseStream",
          "bedrock:ConverseStream",
        ],
        resources: [
          `arn:${cdk.Aws.PARTITION}:bedrock:*::foundation-model/*`,
          `arn:${cdk.Aws.PARTITION}:bedrock:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:inference-profile/*`,
        ],
      })
    );

    // 3. Add Function URL with RESPONSE_STREAM invoke mode
    // Note: Set authType to AWS_IAM for production with SigV4 signing
    const fnUrl = streamingFn.addFunctionUrl({
      authType: lambda.FunctionUrlAuthType.AWS_IAM,
      invokeMode: lambda.InvokeMode.RESPONSE_STREAM,
      cors: {
        allowedOrigins: ["*"],
        allowedMethods: [lambda.HttpMethod.ALL],
        allowedHeaders: ["Content-Type", "Authorization", "x-api-key"],
      },
    });

    // 4. Wrap with CloudFront CDN using FunctionUrlOrigin
    const fnUrlDomain = cdk.Fn.select(2, cdk.Fn.split("/", fnUrl.url));

    const distribution = new cloudfront.Distribution(this, "StreamingCdn", {
      defaultBehavior: {
        origin: new origins.HttpOrigin(fnUrlDomain, {
          protocolPolicy: cloudfront.OriginProtocolPolicy.HTTPS_ONLY,
        }),
        viewerProtocolPolicy: cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
        allowedMethods: cloudfront.AllowedMethods.ALLOW_ALL,
        // CachingDisabled ensures CloudFront does not buffer SSE chunks
        cachePolicy: cloudfront.CachePolicy.CACHING_DISABLED,
        // ALL_VIEWER_EXCEPT_HOST_HEADER (b689b0a8-53d0-40ab-baf2-68738e2966ac)
        originRequestPolicy: cloudfront.OriginRequestPolicy.ALL_VIEWER_EXCEPT_HOST_HEADER,
      },
    });

    // Outputs
    new cdk.CfnOutput(this, "FunctionUrl", {
      value: fnUrl.url,
      description: "Direct Lambda Function URL with Response Streaming",
    });

    new cdk.CfnOutput(this, "CloudFrontUrl", {
      value: `https://${distribution.distributionDomainName}`,
      description: "CloudFront Streaming Endpoint",
    });
  }
}
