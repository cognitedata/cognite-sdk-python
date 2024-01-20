from __future__ import annotations

from abc import ABC
from typing import TYPE_CHECKING, Any

from cognite.client.data_classes._base import (
    CogniteObject,
    CogniteObjectUpdate,
    CognitePrimitiveUpdate,
    CogniteResource,
    CogniteUpdate,
    PropertySpec,
)
from cognite.client.data_classes.user_profiles import UserProfilesConfiguration

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class Claim(CogniteObject):
    """A claim is a property of a token that can be used to grant access to CDF resources.

    Args:
        claim_name (str): The name of the claim.
    """

    def __init__(self, claim_name: str) -> None:
        self.claim_name = claim_name

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Claim:
        return cls(claim_name=resource["claimName"])


class OIDCConfiguration(CogniteObject):
    """OIDC configuration for a project.

    Args:
        jwks_url (str): The URL where the signing keys used to sign tokens from the identity provider are located
        issuer (str): The expected issuer value.
        audience (str): The expected audience value (for CDF).
        access_claims (list[Claim]): Which claims to link CDF groups to, in order to grant access.
        scope_claims (list[Claim]): Which claims to use when scoping access granted by access claims.
        log_claims (list[Claim]): Which token claims to record in the audit log.
        token_url (str | None): The URL of the OAuth 2.0 token endpoint.
        skew_ms (int | None): The allowed skew in milliseconds.
        is_group_callback_enabled (bool | None): A group callback occurs when a user has too many groups attached. This property indicates whether the group callback functionality should be supported for this project. This is only supported for AAD hosted IdPs.
        identity_provider_scope (str | None): The scope sent to the identity provider when a session is created. The default value is the value required for a default Azure AD IdP configuration.

    """

    def __init__(
        self,
        jwks_url: str,
        issuer: str,
        audience: str,
        access_claims: list[Claim],
        scope_claims: list[Claim],
        log_claims: list[Claim],
        token_url: str | None = None,
        skew_ms: int | None = None,
        is_group_callback_enabled: bool | None = None,
        identity_provider_scope: str | None = None,
    ) -> None:
        self.jwks_url = jwks_url
        self.issuer = issuer
        self.audience = audience
        self.access_claims = access_claims
        self.scope_claims = scope_claims
        self.log_claims = log_claims
        self.token_url = token_url
        self.skew_ms = skew_ms
        self.is_group_callback_enabled = is_group_callback_enabled
        self.identity_provider_scope = identity_provider_scope

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> OIDCConfiguration:
        return cls(
            jwks_url=resource["jwksUrl"],
            issuer=resource["issuer"],
            audience=resource["audience"],
            access_claims=[Claim.load(claim) for claim in resource["accessClaims"]],
            scope_claims=[Claim.load(claim) for claim in resource["scopeClaims"]],
            log_claims=[Claim.load(claim) for claim in resource["logClaims"]],
            token_url=resource.get("tokenUrl"),
            skew_ms=resource.get("skewMs"),
            is_group_callback_enabled=resource.get("isGroupCallbackEnabled"),
            identity_provider_scope=resource.get("identityProviderScope"),
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {
            "jwksUrl" if camel_case else "jwks_url": self.jwks_url,
            "issuer": self.issuer,
            "audience": self.audience,
            "accessClaims" if camel_case else "access_claims": [claim.dump(camel_case) for claim in self.access_claims],
            "scopeClaims" if camel_case else "scope_claims": [claim.dump(camel_case) for claim in self.scope_claims],
            "logClaims" if camel_case else "log_claims": [claim.dump(camel_case) for claim in self.log_claims],
        }
        if self.token_url is not None:
            output["tokenUrl" if camel_case else "token_url"] = self.token_url
        if self.skew_ms is not None:
            output["skewMs" if camel_case else "skew_ms"] = self.skew_ms
        if self.is_group_callback_enabled is not None:
            output[
                "isGroupCallbackEnabled" if camel_case else "is_group_callback_enabled"
            ] = self.is_group_callback_enabled
        if self.identity_provider_scope is not None:
            output["identityProviderScope" if camel_case else "identity_provider_scope"] = self.identity_provider_scope
        return output


class ProjectCore(CogniteResource, ABC):
    """Projects are used to isolate data in CDF rom each other. All objects in CDF belong to a single project,
    and objects in different projects are isolated from each other.

    Typically, we would have at least one dev and a prod project. The dev project is used for development and testing,
    while the prod project is used for production data.

    Args:
        name (str): The user-friendly name of the project.
        url_name (str): The URL name of the project. This is used as part of the request path in API calls. Valid URL names contain between 3 and 32 characters, and may only contain English letters, digits and hyphens, must contain at least one letter and may not start or end with a hyphen.
        oidc_configuration (OIDCConfiguration | None): The OIDC configuration for the project.
    """

    def __init__(
        self,
        name: str,
        url_name: str,
        oidc_configuration: OIDCConfiguration | None = None,
    ) -> None:
        self.name = name
        self.url_name = url_name
        self.oidc_configuration = oidc_configuration


class ProjectWrite(ProjectCore):
    """Projects are used to isolate data in CDF rom each other. All objects in CDF belong to a single project,
    and objects in different projects are isolated from each other.

    This is the write format of a Project. It is used when creating a new Project.

    Typically, we would have at least one dev and a prod project. The dev project is used for development and testing,
    while the prod project is used for production data.

    Args:
        name (str): The user-friendly name of the project.
        url_name (str): The URL name of the project. This is used as part of the request path in API calls. Valid URL names contain between 3 and 32 characters, and may only contain English letters, digits and hyphens, must contain at least one letter and may not start or end with a hyphen.
        parent_project_url_name (str): The URL name of the project from which the new project is being created- this project must already exist.
        admin_source_group_id (str | None): ID of the group in the source. If this is the same ID as a group in the IdP, a principal in that group will implicitly be a part of this group as well.
        oidc_configuration (OIDCConfiguration | None): The OIDC configuration for the project.
        user_profiles_configuration (UserProfilesConfiguration | None): Should the collection of user profiles be enabled for the project.
    """

    def __init__(
        self,
        name: str,
        url_name: str,
        parent_project_url_name: str,
        admin_source_group_id: str | None = None,
        oidc_configuration: OIDCConfiguration | None = None,
        user_profiles_configuration: UserProfilesConfiguration | None = None,
    ) -> None:
        super().__init__(name, url_name, oidc_configuration)
        self.parent_project_url_name = parent_project_url_name
        self.admin_source_group_id = admin_source_group_id
        # user_profile_configuration is not in ProjectCore as it
        # is required for the Read format but not the Write format.
        self.user_profiles_configuration = user_profiles_configuration

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> ProjectWrite:
        return cls(
            name=resource["name"],
            url_name=resource["urlName"],
            parent_project_url_name=resource["parentProjectUrlName"],
            admin_source_group_id=resource.get("adminSourceGroupId"),
            oidc_configuration=OIDCConfiguration._load(resource["oidcConfiguration"])
            if "oidcConfiguration" in resource
            else None,
            user_profiles_configuration=UserProfilesConfiguration._load(resource["userProfilesConfiguration"])
            if "userProfilesConfiguration" in resource
            else None,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {
            "name": self.name,
            "urlName" if camel_case else "url_name": self.url_name,
            "parentProjectUrlName" if camel_case else "parent_project_url_name": self.parent_project_url_name,
        }
        if self.admin_source_group_id is not None:
            output["adminSourceGroupId" if camel_case else "admin_source_group_id"] = self.admin_source_group_id
        if self.oidc_configuration is not None:
            output["oidcConfiguration" if camel_case else "oidc_configuration"] = self.oidc_configuration.dump(
                camel_case
            )
        if self.user_profiles_configuration is not None:
            output[
                "userProfilesConfiguration" if camel_case else "user_profiles_configuration"
            ] = self.user_profiles_configuration.dump(camel_case)
        return output


class Project(ProjectCore):
    """Projects are used to isolate data in CDF rom each other. All objects in CDF belong to a single project,
    and objects in different projects are isolated from each other.

    This is the read format of a Project. It is used when retrieving a Project.

    Typically, we would have at least one dev and a prod project. The dev project is used for development and testing,
    while the prod project is used for production data.

    Args:
        name (str): The user-friendly name of the project.
        url_name (str): The URL name of the project. This is used as part of the request path in API calls. Valid URL names contain between 3 and 32 characters, and may only contain English letters, digits and hyphens, must contain at least one letter and may not start or end with a hyphen.
        user_profiles_configuration (UserProfilesConfiguration): Should the collection of user profiles be enabled for the project.
        oidc_configuration (OIDCConfiguration | None): The OIDC configuration for the project.
    """

    def __init__(
        self,
        name: str,
        url_name: str,
        user_profiles_configuration: UserProfilesConfiguration,
        oidc_configuration: OIDCConfiguration | None = None,
    ) -> None:
        super().__init__(name, url_name, oidc_configuration)
        # user_profile_configuration is not in ProjectCore as it
        # is required for the Read format but not the Write format.
        self.user_profiles_configuration = user_profiles_configuration

    @classmethod
    def _load(cls, resource: dict[str, Any], cognite_client: CogniteClient | None = None) -> Project:
        return cls(
            name=resource["name"],
            url_name=resource["urlName"],
            user_profiles_configuration=UserProfilesConfiguration._load(resource["userProfilesConfiguration"]),
            oidc_configuration=OIDCConfiguration._load(resource["oidcConfiguration"])
            if "oidcConfiguration" in resource
            else None,
        )

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        output: dict[str, Any] = {
            "name": self.name,
            "urlName" if camel_case else "url_name": self.url_name,
            "userProfilesConfiguration"
            if camel_case
            else "user_profiles_configuration": self.user_profiles_configuration.dump(camel_case),
        }
        if self.oidc_configuration is not None:
            output["oidcConfiguration" if camel_case else "oidc_configuration"] = self.oidc_configuration.dump(
                camel_case
            )
        return output


class ProjectUpdate(CogniteUpdate):
    """Changes applied to a Project.

    Args:
        project: The Project to be updated.
    """

    class _PrimitiveProjectUpdate(CognitePrimitiveUpdate):
        def set(self, value: str) -> ProjectUpdate:
            return self._set(value)

    class _OIDCProjectUpdate(CogniteObjectUpdate):
        def set(self, value: OIDCConfiguration | None) -> ProjectUpdate:
            return self._set((value and value.dump()) or {})

        def modify(self) -> ProjectUpdate:
            raise NotImplementedError

    class _UserProfileProjectUpdate(CogniteObjectUpdate):
        def set(self, value: UserProfilesConfiguration) -> ProjectUpdate:
            return self._set(value.dump())

        def modify(self) -> ProjectUpdate:
            raise NotImplementedError

    @property
    def project(self) -> ProjectUpdate._PrimitiveProjectUpdate:
        return ProjectUpdate._PrimitiveProjectUpdate(self, "project")

    @property
    def oidc_configuration(self) -> ProjectUpdate._OIDCProjectUpdate:
        return ProjectUpdate._OIDCProjectUpdate(self, "oidcConfiguration")

    @property
    def user_profiles_configuration(self) -> ProjectUpdate._UserProfileProjectUpdate:
        return ProjectUpdate._UserProfileProjectUpdate(self, "userProfilesConfiguration")

    @classmethod
    def _get_update_properties(cls) -> list[PropertySpec]:
        return [
            # External ID is nullable, but is used in the upsert logic and thus cannot be nulled out.
            PropertySpec("project", is_nullable=False),
            PropertySpec("oidc_configuration", is_nullable=True),
            PropertySpec("user_profiles_configuration", is_nullable=False),
        ]
