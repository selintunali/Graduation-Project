select 
	part2.*,ct.cDescription as packdescription,
	ai.cName as indname,
	ai.cAddress1 as indadd1,
	ai.cAddress2 as indadd1,
	ai.cCity as indcity,
	ai.cState as indstate,
	ai.cZip as indzip,
	ai1.cName as inpname,
	ai1.cAddress1 as inpadd1,
	ai1.cAddress2 as inpadd1,
	ai1.cCity as inpcity,
	ai1.cState as inpstate,
	ai1.cZip as inpzip,
	ao.cName as outdname,
	ao.cDetailCassAddress1 as outdadd1,
	ao.cAddress2 as outdadd2,
	ao.cCity as outdcity,
	ao.cState as outdstate,
	ao.czip as outdzip,
	ao.cDetailCassName as outdcassname,
	ao.cDetailCassAddress1 as outdcassadd1, 
	ao.cDetailCassAddress2 as outdcassadd2, 
	ao.cDetailCassCity as outdcasscity, 
	ao.cDetailCassState as outdcassstate, 
	ao.cDetailCassZip as outdcasszip,
	ao1.cName as outpname,
	ao1.cDetailCassAddress1 as outpadd1,
	ao1.cAddress2 as outpadd2,
	ao1.cCity as outpcity,
	ao1.cState as outpstate,
	ao1.czip as outpzip,
	ao1.cDetailCassName as outpcassname,
	ao1.cDetailCassAddress1 as outpcassadd1, 
	ao1.cDetailCassAddress2 as outpcassadd2, 
	ao1.cDetailCassCity as outpcasscity, 
	ao1.cDetailCassState as outpcassstate, 
	ao1.cDetailCassZip as outpcasszip

from  
	Reference.ContainerTypes CT,
	docsys.AddressesInbound AI,
	docsys.AddressesInbound AI1,
	docsys.AddressesOutbound AO,
	docsys.AddressesOutbound AO1,

	(select 
		part1.*,
		dhs.yRetailShippingRate,
		dhs.cShippingAddressInboundId,
		dhs.cShippingAddressOutboundId,
		cPackageContainerType

	from Client1001Format001.DocData.DocumentHistoryShipping DHS,

		(select 
			dhp.cdocid,
			dhp.cBatchNumber,
			dhp.cDeliveryTypeId,
			cDescription,
			dhp.cJobNumber,
			iBarcodePackageNumber,
			iShippingItemNumber,
			iBarcodePackagePageCount,
			iTotalPackagePageCount,
			iDCMPackageType,
			cAddressInboundId,
			cAddressOutboundId

		from 
			Client1001Format001.DocData.DocumentHistoryMaster00000 DHM,
			Client1001Format001.DocData.DocumentHistoryProduction00000 dhp, 
			Reference.DeliveryTypes DT

		where dhp.cdocid = dhm.cdocid
			and (dhp.lPurged is null or dhp.lpurged = '0')
			and (dhp.iversion is null  or dhp.iversion = 0)
			and (lReissuedTouchAndToss is null or lReissuedTouchAndToss = '0')
			and (dhp.cBatchNumber >= '20211201000' and dhp.cBatchNumber <= '20211231ZZZ')
			and cRecipientType = 'P'
			and dhp.cDeliveryTypeId = dt.cDeliveryTypeId
			and lprintable = '1'

		group by 
			dhp.cdocid,
			dhp.cBatchNumber,
			dhp.cDeliveryTypeId,
			cDescription,
			dhp.cJobNumber,
			iBarcodePackageNumber,
			iShippingItemNumber,
			iBarcodePackagePageCount,
			iTotalPackagePageCount,
			iDCMPackageType,
			cAddressInboundId,
			cAddressOutboundId) part1

		where (dhs.cBatchNumber = part1.cBatchNumber 
			and dhs.iShippingItemNumber = part1.iShippingItemNumber)
			and (iversion is null or iversion = 0) ) part2

	where ct.cContainerTypeId = part2.cPackageContainerType
		and AI.cAddressInboundId = part2.cAddressInboundId
		and AI1.cAddressInboundId = part2.cShippingAddressInboundId
		and AO.cAddressOutboundId = part2.cAddressOutboundId
		and AO1.cAddressOutboundId = part2.cShippingAddressOutboundId


		select part2.*,
		ct.cDescription as packdescription,
		ai.cName as indname,
		ai.cAddress1 as indadd1,
		ai.cAddress2 as indadd1,
		ai.cCity as indcity,
		ai.cState as indstate,
		ai.cZip as indzip,
		ai1.cName as inpname,
		ai1.cAddress1 as inpadd1,
		ai1.cAddress2 as inpadd1,
		ai1.cCity as inpcity,
		ai1.cState as inpstate,
		ai1.cZip as inpzip,
		ao.cName as outdname,
		ao.cDetailCassAddress1 as outdadd1,
		ao.cAddress2 as outdadd2,
		ao.cCity as outdcity,
		ao.cState as outdstate,
		ao.czip as outdzip,
		ao.cDetailCassName as outdcassname,
		ao.cDetailCassAddress1 as outdcassadd1, 
		ao.cDetailCassAddress2 as outdcassadd2, 
		ao.cDetailCassCity as outdcasscity, 
		ao.cDetailCassState as outdcassstate, 
		ao.cDetailCassZip as outdcasszip,
		ao1.cName as outpname,
		ao1.cDetailCassAddress1 as outpadd1,
		ao1.cAddress2 as outpadd2,
		ao1.cCity as outpcity,
		ao1.cState as outpstate,
		ao1.czip as outpzip,
		ao1.cDetailCassName as outpcassname,
		ao1.cDetailCassAddress1 as outpcassadd1, 
		ao1.cDetailCassAddress2 as outpcassadd2, 
		ao1.cDetailCassCity as outpcasscity, 
		ao1.cDetailCassState as outpcassstate, 
		ao1.cDetailCassZip as outpcasszip

		from  
			Reference.ContainerTypes CT,
			docsys.AddressesInbound AI,
			docsys.AddressesInbound AI1,
			docsys.AddressesOutbound AO,
			docsys.AddressesOutbound AO1,

			(select 
				part1.*,
				dhs.yRetailShippingRate,
				dhs.cShippingAddressInboundId,
				dhs.cShippingAddressOutboundId,
				cPackageContainerType

			from 
				' + @DB_Name + '.DocData.DocumentHistoryShipping DHS,

				(select dhp.cdocid,
					dhp.cBatchNumber,
					dhp.cDeliveryTypeId,
					cDescription,
					dhp.cJobNumber,
					iBarcodePackageNumber,
					iShippingItemNumber,
					iBarcodePackagePageCount,
					iTotalPackagePageCount,
					iDCMPackageType,
					cAddressInboundId,
					cAddressOutboundId

				where dhp.cdocid = dhm.cdocid
				and (dhp.lPurged is null or dhp.lpurged = ''0'')
				and (dhp.iversion is null  or dhp.iversion = 0)
				and (lReissuedTouchAndToss is null or lReissuedTouchAndToss = ''0'')
				and (dhp.cBatchNumber >= ''20211201000'' and dhp.cBatchNumber <= ''20211231ZZZ'')
				and cRecipientType = ''P''
				and dhp.cDeliveryTypeId = dt.cDeliveryTypeId
				and lprintable = ''1''

				group by 
					dhp.cdocid,
					dhp.cBatchNumber,
					dhp.cDeliveryTypeId,
					cDescription,
					dhp.cJobNumber,
					iBarcodePackageNumber,
					iShippingItemNumber,
					iBarcodePackagePageCount,
					iTotalPackagePageCount,
					iDCMPackageType,
					cAddressInboundId,
					cAddressOutboundId) part1

			where (dhs.cBatchNumber = part1.cBatchNumber 
				and dhs.iShippingItemNumber = part1.iShippingItemNumber)
				and (iversion is null or iversion = 0) ) part2

		where ct.cContainerTypeId = part2.cPackageContainerType
		and AI.cAddressInboundId = part2.cAddressInboundId
		and AI1.cAddressInboundId = part2.cShippingAddressInboundId
		and AO.cAddressOutboundId = part2.cAddressOutboundId
		and AO1.cAddressOutboundId = part2.cShippingAddressOutboundId










